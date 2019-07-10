package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/badoux/checkmail"
	"github.com/ishanjain28/csvd"
	"sourcegraph.com/sourcegraph/go-diff/diff"
)

type recordType string

const (
	// UPDATED is for when a record is updated
	UPDATED recordType = "UPDATED"
	// DELETED is for when a record is deleted
	DELETED recordType = "DELETED"
	// CREATED is for when a record is created
	CREATED recordType = "CREATED"
)

// Record represents the type of a change and the records involved in that change
type Record struct {
	Type recordType
	// AssociatedRecords can be at max of length 2. i.e. in case a record was updated
	AssociatedRecords [2][]string
}

func main() {

	tableName := os.Getenv("TABLE_NAME")
	if tableName == "" {
		log.Fatalln("$TABLE_NAME is not set")
	}

	originalFilePathGSI := os.Getenv("ORIGINAL_FILEPATH_GSI")
	if originalFilePathGSI == "" {
		log.Fatalln("$ORIGINAL_FILEPATH_GSI is not set")
	}

	sess := session.Must(session.NewSession())
	s3Svc := s3.New(sess)
	dynamoSvc := dynamodb.New(sess)

	lambda.Start(func(ip events.SQSEvent) error {
		return diffAndSave(tableName, originalFilePathGSI, ip, s3Svc, dynamoSvc)
	})
}

func diffAndSave(tableName, originalFilePathGSI string, sqsEvt events.SQSEvent, svc *s3.S3, dynamoSvc dynamodbiface.DynamoDBAPI) error {
	for _, record := range sqsEvt.Records {
		// sqsRecord := record.SQS
		// fmt.Printf("[%s %s] SNS Message = %s \n", record.EventSource, sqsRecord.Timestamp, sqsRecord.Message)
		fmt.Printf("Record Message = %s \n", record)
		snsEntity := events.SNSEntity{}
		err := json.Unmarshal([]byte(record.Body), &snsEntity)
		if err != nil {
			return err
		}

		s3Evt := events.S3Event{}
		err = json.Unmarshal([]byte(snsEntity.Message), &s3Evt)
		if err != nil {
			return err
		}

		for _, evt := range s3Evt.Records {
			filename := path.Base(evt.S3.Object.Key)

			switch path.Ext(filename) {
			case ".csv", ".tsv", ".txt":
			default:
				continue
			}

			upload, archive, err := getFiles(evt, svc)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case s3.ErrCodeNoSuchKey:
						fmt.Printf("%s not found in archive. Skipping\n", filename)
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					fmt.Println(err.Error())
				}
				continue
			}

			evtFileName := fmt.Sprintf("s3://%s/%s#%s", evt.S3.Bucket.Name, evt.S3.Object.Key, evt.S3.Object.VersionID)

			err = updateStatusInDynamoDB(dynamoSvc, tableName, originalFilePathGSI, evtFileName)
			if err != nil {
				fmt.Printf("error in updating status in DynamoDB for %s: %v", evtFileName, err)
			}
			idRecordMapping := map[string]*Record{}

			uploadFileBody, err := ioutil.ReadAll(upload.Body)
			if err != nil {
				return fmt.Errorf("error in reading %s: %v", evtFileName, err)
			}
			if archive == nil {
				// If the archive doesn't exists, All records are treated as "CREATED"
				idRecordMapping, err = generateIDRecordMappingFromUploadOnly(bytes.NewReader(uploadFileBody))
				if err != nil {
					if err == io.EOF {
						fmt.Printf("%s is empty", evtFileName)
						break
					}
					return err
				}
			} else {
				uploadTempFile, err := ioutil.TempFile("", "upload")
				if err != nil {
					log.Fatalln(err)
				}
				archiveTempFile, err := ioutil.TempFile("", "archive")
				if err != nil {
					log.Fatalln(err)
				}

				_, err = io.Copy(uploadTempFile, bytes.NewReader(uploadFileBody))
				if err != nil {
					return fmt.Errorf("error in saving %s: %v", evtFileName, err)
				}
				_, err = io.Copy(archiveTempFile, archive.Body)
				if err != nil {
					return fmt.Errorf("error in saving %s: %v", evtFileName, err)
				}
				idRecordMapping, err = generateIDRecordMapping(archiveTempFile.Name(), uploadTempFile.Name(), "/opt/diff")
				if err != nil {
					if err == io.EOF {
						fmt.Printf("%s Versions: [%s %s] are the same", evt.S3.Object.Key, aws.StringValue(upload.VersionId), aws.StringValue(archive.VersionId))
						break
					}
					return err
				}
			}

			// Save files to S3 in a processed folder
			err = saveRecords(svc, evt.S3.Bucket.Name, filename, idRecordMapping)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getFiles(evt events.S3EventRecord, svc s3iface.S3API) (*s3.GetObjectOutput, *s3.GetObjectOutput, error) {
	versionsls, err := svc.ListObjectVersions(&s3.ListObjectVersionsInput{
		Bucket: aws.String(evt.S3.Bucket.Name),
		Prefix: aws.String(evt.S3.Object.Key),
		// Return only the two most recent versions
		MaxKeys: aws.Int64(2),
	})
	if err != nil {
		return nil, nil, err
	}
	if len(versionsls.Versions) == 0 {
		return nil, nil, awserr.New(s3.ErrCodeNoSuchKey, "The specified key does not exist.", nil)
	}

	upload, err := svc.GetObject(&s3.GetObjectInput{
		Bucket:    aws.String(evt.S3.Bucket.Name),
		Key:       aws.String(evt.S3.Object.Key),
		VersionId: versionsls.Versions[0].VersionId,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(versionsls.Versions) == 1 {
		return upload, nil, nil
	}

	archive, err := svc.GetObject(&s3.GetObjectInput{
		Bucket:    aws.String(evt.S3.Bucket.Name),
		Key:       aws.String(evt.S3.Object.Key),
		VersionId: versionsls.Versions[1].VersionId,
	})
	if err != nil {
		return nil, nil, err
	}

	return upload, archive, nil
}

func saveRecords(s s3iface.S3API, bucket, key string, records map[string]*Record) error {
	var addedWriterBuf bytes.Buffer
	var deletedWriterBuf bytes.Buffer
	var updatedWriterBuf bytes.Buffer

	addedWriter := csv.NewWriter(&addedWriterBuf)
	deletedWriter := csv.NewWriter(&deletedWriterBuf)
	updatedWriter := csv.NewWriter(&updatedWriterBuf)

	for _, v := range records {
		switch v.Type {
		case UPDATED:
			// In case of records that were updated, Both older and newer version of a record is saved in idRecordMapping
			// But I am not sure what should we do about the older record.
			// So, Here the newer record is saved to S3
			// And later, All the records in <key>_updated.csv file will be updated in dynamo/<etc> with a update operation
			updatedWriter.Write(v.AssociatedRecords[1])
		case DELETED:
			deletedWriter.Write(v.AssociatedRecords[0])
		case CREATED:
			addedWriter.Write(v.AssociatedRecords[0])
		}
	}
	updatedWriter.Flush()
	addedWriter.Flush()
	deletedWriter.Flush()

	if len(addedWriterBuf.Bytes()) != 0 {
		_, err := s.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(path.Join("created", key)),
			Body:        bytes.NewReader(addedWriterBuf.Bytes()),
			ContentType: aws.String("text/csv; charset=utf-8"),
		})
		if err != nil {
			return err
		}
	}
	if len(deletedWriterBuf.Bytes()) != 0 {
		_, err := s.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(path.Join("deleted", key)),
			Body:        bytes.NewReader(deletedWriterBuf.Bytes()),
			ContentType: aws.String("text/csv; charset=utf-8"),
		})
		if err != nil {
			return err
		}
	}
	if len(updatedWriterBuf.Bytes()) != 0 {
		_, err := s.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(path.Join("updated", key)),
			Body:        bytes.NewReader(updatedWriterBuf.Bytes()),
			ContentType: aws.String("text/csv; charset=utf-8"),
		})
		if err != nil {
			return err
		}
	}
	return nil
}
func generateIDRecordMappingFromUploadOnly(body io.Reader) (map[string]*Record, error) {
	idRecordMapping := map[string]*Record{}
	records := make(chan []string)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func(idRecordMapping map[string]*Record, records chan []string, wg *sync.WaitGroup) {
		defer wg.Done()
		for h := range records {
			idRecordMapping[h[0]] = &Record{
				Type:              CREATED,
				AssociatedRecords: [2][]string{h},
			}
		}
	}(idRecordMapping, records, &wg)

	err := parseRecords(body, records)
	if err != nil {
		return nil, err
	}

	return idRecordMapping, nil
}

func generateIDRecordMapping(archiveTempFileName, uploadTempFileName, diffExecPath string) (map[string]*Record, error) {
	out, err := runDiff(archiveTempFileName, uploadTempFileName, diffExecPath)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, io.EOF
	}

	d, err := diff.ParseFileDiff(out)
	if err != nil {
		return nil, fmt.Errorf("error in parsing diff output: %s", err)
	}

	idRecordMapping := map[string]*Record{}
	for _, h := range d.Hunks {
		processHunk(h, idRecordMapping)
	}

	return idRecordMapping, nil
}

func processHunk(h *diff.Hunk, idRecordMapping map[string]*Record) {

	records := make(chan []string)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func(idRecordMapping map[string]*Record, records chan []string, wg *sync.WaitGroup) {
		defer wg.Done()
		for record := range records {
			switch record[0][0] {
			case '+':
				id := record[0][1:]
				if _, ok := idRecordMapping[id]; ok {
					if idRecordMapping[id].Type == DELETED {
						idRecordMapping[id].Type = UPDATED
					}

					idRecordMapping[id].AssociatedRecords[1] = append([]string{id}, record[1:]...)
				} else {
					idRecordMapping[id] = &Record{
						AssociatedRecords: [2][]string{append([]string{id}, record[1:]...)},
						Type:              CREATED,
					}
				}
			case '-':
				id := record[0][1:]
				if _, ok := idRecordMapping[id]; ok {
					if idRecordMapping[id].Type == CREATED {
						idRecordMapping[id].Type = UPDATED
					}

					idRecordMapping[id].AssociatedRecords[1] = append([]string{id}, record[1:]...)
				} else {
					idRecordMapping[id] = &Record{
						AssociatedRecords: [2][]string{append([]string{id}, record[1:]...)},
						Type:              DELETED,
					}
				}
			}
		}
	}(idRecordMapping, records, &wg)

	err := parseRecords(bytes.NewReader(h.Body), records)
	if err != nil {
		return
	}
}

func runDiff(file1, file2, diffAddr string) ([]byte, error) {
	cmd := exec.Command(diffAddr, "-u", file1, file2)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if err.Error() != "exit status 1" {
			fmt.Println(string(out))
			return nil, err
		}
	}
	return out, nil
}

func parseRecords(csvData io.Reader, outputChan chan []string) error {
	defer close(outputChan)
	b, err := ioutil.ReadAll(csvData)
	if err != nil {
		return err
	}
	bytesReader := bytes.NewReader(b)

	sniffer := csvd.NewSniffer(50, '|', ',', '\t', ';', ':')

	// DetectDelimiter reads a small part of the csv and then Seeks to 0,0 to bring the reader back to the starting position
	delimiter := csvd.DetectDelimiter(bytesReader, sniffer)
	reader := csv.NewReader(bytesReader)
	reader.LazyQuotes = true
	reader.Comma = delimiter

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			fmt.Printf("PARSE_ERROR: Delimiter(%s) Record(%s): %v\n", strconv.QuoteRune(delimiter), record, err)
		} else {
			if err = checkmail.ValidateFormat(record[1]); err == nil {
				outputChan <- record
			}
		}
	}
	return nil
}

func updateStatusInDynamoDB(svc dynamodbiface.DynamoDBAPI, tableName, originalFilePathGSI, evtFileName string) error {

	// Find out uuid for a given value of `original`
	resultSet, err := svc.Query(&dynamodb.QueryInput{
		TableName: aws.String(tableName),
		IndexName: aws.String(originalFilePathGSI),
		// No need to apply a clause to check job_status = UPLOADED since UUID's are
		// created new for each S3 Event regardless of their end result. i.e.
		// Regardless of the situation in which the uploads function fails for that event
		KeyConditionExpression: aws.String("original = :o"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":o": &dynamodb.AttributeValue{S: aws.String(evtFileName)},
		},
	})
	if err != nil {
		return err
	}
	if aws.Int64Value(resultSet.Count) == 0 {
		return fmt.Errorf("no record found")
	}
	// Read the uuid field of first record from resultSet
	u := resultSet.Items[0]["uuid"].S

	_, err = svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"uuid": &dynamodb.AttributeValue{S: u},
		},
		UpdateExpression: aws.String("set job_status = :s"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": &dynamodb.AttributeValue{S: aws.String("PROCESSED")},
		},
	})

	return err
}

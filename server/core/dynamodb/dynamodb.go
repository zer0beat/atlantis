// Package dynamodb handles our remote database layer.
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
)

var ctx = context.Background()

type DynamoDB struct {
	Client *dynamodb.DynamoDB
	Table  string
}

const (
	pullKeySeparator = "::"
)

func New(region string, table string) (*DynamoDB, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	ddb := dynamodb.New(sess)

	return &DynamoDB{
		Client: ddb,
		Table:  table,
	}, nil
}

// NewWithClient is used for testing.
func NewWithClient(client *dynamodb.DynamoDB, table string) (*DynamoDB, error) {
	return &DynamoDB{
		Client: client,
		Table:  table,
	}, nil
}

// TryLock attempts to create a new lock. If the lock is
// acquired, it will return true and the lock returned will be newLock.
// If the lock is not acquired, it will return false and the current
// lock that is preventing this lock from being acquired.
func (d *DynamoDB) TryLock(newLock models.ProjectLock) (bool, models.ProjectLock, error) {
	var currLock models.ProjectLock
	lockID, lockType := d.lockKey(newLock.Project, newLock.Workspace)
	newLockSerialized, _ := json.Marshal(newLock)

	putParams := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"LockID":   {S: aws.String(lockID)},
			"LockType": {S: aws.String(lockType)},
			"Info":     {S: aws.String(string(newLockSerialized))},
		},
		TableName:           aws.String(d.Table),
		ConditionExpression: aws.String("attribute_not_exists(LockID)"),
	}
	_, err := d.Client.PutItem(putParams)

	if err != nil {
		getParams := &dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				"LockID": {S: aws.String(lockID)},
			},
			ProjectionExpression: aws.String("LockID, Info"),
			TableName:            aws.String(d.Table),
			ConsistentRead:       aws.Bool(true),
		}

		getResp, err := d.Client.GetItem(getParams)

		if err != nil {
			return false, currLock, errors.Wrap(err, "db transaction failed")
		}

		var val string
		if v, ok := getResp.Item["Info"]; ok && v.S != nil {
			val = *v.S
		}

		if err := json.Unmarshal([]byte(val), &currLock); err != nil {
			return false, currLock, errors.Wrap(err, "failed to deserialize current lock")
		}

		return false, currLock, nil
	}

	return true, newLock, nil
}

// Unlock attempts to unlock the project and workspace.
// If there is no lock, then it will return a nil pointer.
// If there is a lock, then it will delete it, and then return a pointer
// to the deleted lock.
func (d *DynamoDB) Unlock(project models.Project, workspace string) (*models.ProjectLock, error) {
	var lock models.ProjectLock
	lockID, _ := d.lockKey(project, workspace)

	getParams := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockID)},
		},
		ProjectionExpression: aws.String("LockID, Info"),
		TableName:            aws.String(d.Table),
		ConsistentRead:       aws.Bool(true),
	}

	getResp, err := d.Client.GetItem(getParams)

	if err != nil {
		return nil, errors.Wrap(err, "db transaction failed")
	}

	var val string
	if v, ok := getResp.Item["Info"]; ok && v.S != nil {
		val = *v.S
	}

	if val == "" {
		return nil, err
	}

	if err := json.Unmarshal([]byte(val), &lock); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize current lock")
	}

	delParams := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockID)},
		},
		TableName: aws.String(d.Table),
	}

	_, err = d.Client.DeleteItem(delParams)

	if err != nil {
		return nil, errors.Wrap(err, "db transaction failed")
	}

	return &lock, nil
}

// List lists all current locks.
func (d *DynamoDB) List() ([]models.ProjectLock, error) {
	var locks []models.ProjectLock

	filt := expression.Name("LockType").Equal(expression.Value("pr"))
	proj := expression.NamesList(expression.Name("LockID"), expression.Name("Info"))

	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		log.Fatalf("Got error building expression: %s", err)
	}

	scanInput := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ConsistentRead:            aws.Bool(true),
		TableName:                 aws.String(d.Table),
	}

	scanResp, err := d.Client.Scan(scanInput)

	if err != nil {
		return locks, errors.Wrap(err, "db transaction failed")
	}

	for _, item := range scanResp.Items {
		var lock models.ProjectLock

		var lockID string
		if v, ok := item["lockID"]; ok && v.S != nil {
			lockID = *v.S
		}

		var info string
		if v, ok := item["Info"]; ok && v.S != nil {
			info = *v.S
		}

		if err := json.Unmarshal([]byte(info), &lock); err != nil {
			return locks, errors.Wrap(err, fmt.Sprintf("failed to deserialize lock at key '%s'", lockID))
		}

		locks = append(locks, lock)
	}

	return locks, nil
}

// GetLock returns a pointer to the lock for that project and workspace.
// If there is no lock, it returns a nil pointer.
func (d *DynamoDB) GetLock(project models.Project, workspace string) (*models.ProjectLock, error) {
	lockID, _ := d.lockKey(project, workspace)

	getParams := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockID)},
		},
		ProjectionExpression: aws.String("LockID, Info"),
		TableName:            aws.String(d.Table),
		ConsistentRead:       aws.Bool(true),
	}

	getResp, err := d.Client.GetItem(getParams)

	if err != nil {
		return nil, errors.Wrap(err, "db transaction failed")
	}

	var val string
	if v, ok := getResp.Item["Info"]; ok && v.S != nil {
		val = *v.S
	}

	var lock models.ProjectLock
	if err := json.Unmarshal([]byte(val), &lock); err != nil {
		return nil, errors.Wrapf(err, "deserializing lock at key %q", lockID)
	}

	// need to set it to Local after deserialization due to https://github.com/golang/go/issues/19486
	lock.Time = lock.Time.Local()
	return &lock, nil
}

// UnlockByPull deletes all locks associated with that pull request and returns them.
func (d *DynamoDB) UnlockByPull(repoFullName string, pullNum int) ([]models.ProjectLock, error) {
	var locks []models.ProjectLock

	filt := expression.Name("LockType").Equal(expression.Value("pr"))
	prefix := expression.Name("LockID").BeginsWith(repoFullName)
	proj := expression.NamesList(expression.Name("LockID"), expression.Name("Info"))

	expr, err := expression.NewBuilder().WithFilter(filt).WithFilter(prefix).WithProjection(proj).Build()
	if err != nil {
		log.Fatalf("Got error building expression: %s", err)
	}

	scanInput := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ConsistentRead:            aws.Bool(true),
		TableName:                 aws.String(d.Table),
	}

	scanResp, err := d.Client.Scan(scanInput)

	if err != nil {
		return locks, errors.Wrap(err, "db transaction failed")
	}

	for _, item := range scanResp.Items {
		var lock models.ProjectLock

		var lockID string
		if v, ok := item["lockID"]; ok && v.S != nil {
			lockID = *v.S
		}

		var info string
		if v, ok := item["Info"]; ok && v.S != nil {
			info = *v.S
		}

		if err := json.Unmarshal([]byte(info), &lock); err != nil {
			return locks, errors.Wrapf(err, "deserializing lock at key %q", string(lockID))
		}

		if lock.Pull.Num == pullNum {
			locks = append(locks, lock)
		}
	}

	// delete the locks
	for _, lock := range locks {
		if _, err = d.Unlock(lock.Project, lock.Workspace); err != nil {
			return locks, errors.Wrapf(err, "unlocking repo %s, path %s, workspace %s", lock.Project.RepoFullName, lock.Project.Path, lock.Workspace)
		}
	}

	return locks, nil
}

func (d *DynamoDB) LockCommand(cmdName command.Name, lockTime time.Time) (*command.Lock, error) {

	lock := command.Lock{
		CommandName: cmdName,
		LockMetadata: command.LockMetadata{
			UnixTime: lockTime.Unix(),
		},
	}

	lockKey, lockType := d.commandLockKey(cmdName)

	newLockSerialized, _ := json.Marshal(lock)

	putParams := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"LockID":   {S: aws.String(lockKey)},
			"LockType": {S: aws.String(lockType)},
			"Info":     {S: aws.String(string(newLockSerialized))},
		},
		TableName:           aws.String(d.Table),
		ConditionExpression: aws.String("attribute_not_exists(LockID)"),
	}
	_, err := d.Client.PutItem(putParams)

	if err != nil {
		return nil, errors.Wrap(errors.New("lock already exists"), "db transaction failed")
	}

	return &lock, nil
}

func (d *DynamoDB) UnlockCommand(cmdName command.Name) error {
	lockId, _ := d.commandLockKey(cmdName)

	getParams := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockId)},
		},
		ProjectionExpression: aws.String("LockID, Info"),
		TableName:            aws.String(d.Table),
		ConsistentRead:       aws.Bool(true),
	}

	getResp, err := d.Client.GetItem(getParams)

	if err != nil {
		return errors.Wrap(err, "failed to retrieve lock")
	}

	if getResp.Item == nil {
		return errors.New("db transaction failed: no lock exists")
	}

	deleteParams := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockId)},
		},
		TableName: aws.String(d.Table),
	}

	d.Client.DeleteItem(deleteParams)

	return nil
}

func (d *DynamoDB) CheckCommandLock(cmdName command.Name) (*command.Lock, error) {
	cmdLock := command.Lock{}

	lockId, _ := d.commandLockKey(cmdName)

	getParams := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(lockId)},
		},
		ProjectionExpression: aws.String("LockID, Info"),
		TableName:            aws.String(d.Table),
		ConsistentRead:       aws.Bool(true),
	}

	resp, err := d.Client.GetItem(getParams)

	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve lock")
	}

	if resp.Item == nil {
		return nil, nil
	}

	var val string
	if v, ok := resp.Item["Info"]; ok && v.S != nil {
		val = *v.S
	}

	if err := json.Unmarshal([]byte(val), &cmdLock); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize Lock")
	}

	return &cmdLock, nil
}

// UpdatePullWithResults updates pull's status with the latest project results.
// It returns the new PullStatus object.
func (d *DynamoDB) UpdateProjectStatus(pull models.PullRequest, workspace string, repoRelDir string, newStatus models.ProjectPlanStatus) error {
	key, err := d.pullKey(pull)
	if err != nil {
		return err
	}

	currStatusPtr, err := d.getPull(key)
	if err != nil {
		return err
	}
	if currStatusPtr == nil {
		return nil
	}
	currStatus := *currStatusPtr

	// Update the status.
	for i := range currStatus.Projects {
		// NOTE: We're using a reference here because we are
		// in-place updating its Status field.
		proj := &currStatus.Projects[i]
		if proj.Workspace == workspace && proj.RepoRelDir == repoRelDir {
			proj.Status = newStatus
			break
		}
	}

	err = d.writePull(key, currStatus)
	return errors.Wrap(err, "db transaction failed")
}

func (d *DynamoDB) GetPullStatus(pull models.PullRequest) (*models.PullStatus, error) {
	key, err := d.pullKey(pull)
	if err != nil {
		return nil, err
	}

	pullStatus, err := d.getPull(key)

	return pullStatus, errors.Wrap(err, "db transaction failed")
}

func (d *DynamoDB) DeletePullStatus(pull models.PullRequest) error {
	key, err := d.pullKey(pull)
	if err != nil {
		return err
	}
	return errors.Wrap(d.deletePull(key), "db transaction failed")
}

func (d *DynamoDB) UpdatePullWithResults(pull models.PullRequest, newResults []command.ProjectResult) (models.PullStatus, error) {
	key, err := d.pullKey(pull)
	if err != nil {
		return models.PullStatus{}, err
	}

	var newStatus models.PullStatus
	currStatus, err := d.getPull(key)
	if err != nil {
		return newStatus, errors.Wrap(err, "db transaction failed")
	}

	// If there is no pull OR if the pull we have is out of date, we
	// just write a new pull.
	if currStatus == nil || currStatus.Pull.HeadCommit != pull.HeadCommit {
		var statuses []models.ProjectStatus
		for _, res := range newResults {
			statuses = append(statuses, d.projectResultToProject(res))
		}
		newStatus = models.PullStatus{
			Pull:     pull,
			Projects: statuses,
		}
	} else {
		// If there's an existing pull at the right commit then we have to
		// merge our project results with the existing ones. We do a merge
		// because it's possible a user is just applying a single project
		// in this command and so we don't want to delete our data about
		// other projects that aren't affected by this command.
		newStatus = *currStatus
		for _, res := range newResults {
			// First, check if we should update any existing projects.
			updatedExisting := false
			for i := range newStatus.Projects {
				// NOTE: We're using a reference here because we are
				// in-place updating its Status field.
				proj := &newStatus.Projects[i]
				if res.Workspace == proj.Workspace &&
					res.RepoRelDir == proj.RepoRelDir &&
					res.ProjectName == proj.ProjectName {

					proj.Status = res.PlanStatus()
					updatedExisting = true
					break
				}
			}

			if !updatedExisting {
				// If we didn't update an existing project, then we need to
				// add this because it's a new one.
				newStatus.Projects = append(newStatus.Projects, d.projectResultToProject(res))
			}
		}
	}

	// Now, we overwrite the key with our new status.
	return newStatus, errors.Wrap(d.writePull(key, newStatus), "db transaction failed")
}

func (d *DynamoDB) getPull(key string) (*models.PullStatus, error) {
	getParams := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(key)},
		},
		ProjectionExpression: aws.String("LockID, Info"),
		TableName:            aws.String(d.Table),
		ConsistentRead:       aws.Bool(true),
	}

	getResp, err := d.Client.GetItem(getParams)

	if err != nil {
		return nil, errors.Wrap(err, "db transaction failed")
	}

	var val string
	if v, ok := getResp.Item["Info"]; ok && v.S != nil {
		val = *v.S
	}

	if val == "" {
		return nil, nil
	}

	var p models.PullStatus
	if err := json.Unmarshal([]byte(val), &p); err != nil {
		return nil, errors.Wrapf(err, "deserializing pull at %q with contents %q", key, val)
	}

	return &p, nil
}

func (d *DynamoDB) writePull(key string, pull models.PullStatus) error {
	serialized, err := json.Marshal(pull)
	if err != nil {
		return errors.Wrap(err, "serializing")
	}

	putParams := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"LockID":   {S: aws.String(key)},
			"LockType": {S: aws.String("pull")},
			"Info":     {S: aws.String(string(serialized))},
		},
		TableName:           aws.String(d.Table),
		ConditionExpression: aws.String("attribute_not_exists(LockID)"),
	}
	_, err = d.Client.PutItem(putParams)

	if err != nil {
		return errors.Wrap(errors.New("lock already exists"), "db transaction failed")
	}

	return nil
}

func (d *DynamoDB) deletePull(key string) error {
	delParams := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"LockID": {S: aws.String(key)},
		},
		TableName: aws.String(d.Table),
	}

	_, err := d.Client.DeleteItem(delParams)

	return errors.Wrap(err, "DB Transaction failed")
}

func (d *DynamoDB) lockKey(p models.Project, workspace string) (string, string) {
	return fmt.Sprintf("%s/%s/%s", p.RepoFullName, p.Path, workspace), "pr"
}

func (d *DynamoDB) commandLockKey(cmdName command.Name) (string, string) {
	return fmt.Sprintf("%s/lock", cmdName), "cmd"
}

func (d *DynamoDB) pullKey(pull models.PullRequest) (string, error) {
	hostname := pull.BaseRepo.VCSHost.Hostname
	if strings.Contains(hostname, pullKeySeparator) {
		return "", fmt.Errorf("vcs hostname %q contains illegal string %q", hostname, pullKeySeparator)
	}
	repo := pull.BaseRepo.FullName
	if strings.Contains(repo, pullKeySeparator) {
		return "", fmt.Errorf("repo name %q contains illegal string %q", hostname, pullKeySeparator)
	}

	return fmt.Sprintf("%s::%s::%d", hostname, repo, pull.Num), nil
}

func (d *DynamoDB) projectResultToProject(p command.ProjectResult) models.ProjectStatus {
	return models.ProjectStatus{
		Workspace:   p.Workspace,
		RepoRelDir:  p.RepoRelDir,
		ProjectName: p.ProjectName,
		Status:      p.PlanStatus(),
	}
}

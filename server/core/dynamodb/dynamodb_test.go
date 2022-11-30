package dynamodb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	dynamodb_aws "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/runatlantis/atlantis/server/core/dynamodb"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"

	. "github.com/runatlantis/atlantis/testing"
)

var project = models.NewProject("owner/repo", "parent/child")
var workspace = "default"
var pullNum = 1
var lock = models.ProjectLock{
	Pull: models.PullRequest{
		Num: pullNum,
	},
	User: models.User{
		Username: "lkysow",
	},
	Workspace: workspace,
	Project:   project,
	Time:      time.Now(),
}

func TestLockCommandNotSet(t *testing.T) {
	t.Log("retrieving apply lock when there are none should return empty LockCommand")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	exists, err := ddb.CheckCommandLock(command.Apply)
	Ok(t, err)
	Assert(t, exists == nil, "exp nil")
}

func TestLockCommandEnabled(t *testing.T) {
	t.Log("setting the apply lock")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	timeNow := time.Now()
	_, err := ddb.LockCommand(command.Apply, timeNow)
	Ok(t, err)

	config, err := ddb.CheckCommandLock(command.Apply)
	Ok(t, err)
	Equals(t, true, config.IsLocked())
}

func TestLockCommandFail(t *testing.T) {
	t.Log("setting the apply lock")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	timeNow := time.Now()
	_, err := ddb.LockCommand(command.Apply, timeNow)
	Ok(t, err)

	_, err = ddb.LockCommand(command.Apply, timeNow)
	ErrEquals(t, "db transaction failed: lock already exists", err)
}

func TestUnlockCommandDisabled(t *testing.T) {
	t.Log("unsetting the apply lock")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	timeNow := time.Now()
	_, err := ddb.LockCommand(command.Apply, timeNow)
	Ok(t, err)

	config, err := ddb.CheckCommandLock(command.Apply)
	Ok(t, err)
	Equals(t, true, config.IsLocked())

	err = ddb.UnlockCommand(command.Apply)
	Ok(t, err)

	config, err = ddb.CheckCommandLock(command.Apply)
	Ok(t, err)
	Assert(t, config == nil, "exp nil object")
}

func TestUnlockCommandFail(t *testing.T) {
	t.Log("setting the apply lock")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	err := ddb.UnlockCommand(command.Apply)
	ErrEquals(t, "db transaction failed: no lock exists", err)
}

func TestMixedLocksPresent(t *testing.T) {
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	timeNow := time.Now()
	_, err := ddb.LockCommand(command.Apply, timeNow)
	Ok(t, err)

	_, _, err = ddb.TryLock(lock)
	Ok(t, err)

	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 1, len(ls))
}

func TestListNoLocks(t *testing.T) {
	t.Log("listing locks when there are none should return an empty list")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))
}

func TestListOneLock(t *testing.T) {
	t.Log("listing locks when there is one should return it")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 1, len(ls))
}

func TestListMultipleLocks(t *testing.T) {
	t.Log("listing locks when there are multiple should return them")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	// add multiple locks
	repos := []string{
		"owner/repo1",
		"owner/repo2",
		"owner/repo3",
		"owner/repo4",
	}

	for _, r := range repos {
		newLock := lock
		newLock.Project = models.NewProject(r, "path")
		_, _, err := ddb.TryLock(newLock)
		Ok(t, err)
	}
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 4, len(ls))
	for _, r := range repos {
		found := false
		for _, l := range ls {
			if l.Project.RepoFullName == r {
				found = true
			}
		}
		Assert(t, found, "expected %s in %v", r, ls)
	}
}

func TestListAddRemove(t *testing.T) {
	t.Log("listing after adding and removing should return none")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)
	_, err = ddb.Unlock(project, workspace)
	Ok(t, err)

	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))
}

func TestLockingNoLocks(t *testing.T) {
	t.Log("with no locks yet, lock should succeed")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	acquired, currLock, err := ddb.TryLock(lock)
	Ok(t, err)
	Equals(t, true, acquired)
	Equals(t, lock, currLock)
}

func TestLockingExistingLock(t *testing.T) {
	t.Log("if there is an existing lock, lock should...")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)

	t.Log("...succeed if the new project has a different path")
	{
		newLock := lock
		newLock.Project = models.NewProject(project.RepoFullName, "different/path")
		acquired, currLock, err := ddb.TryLock(newLock)
		Ok(t, err)
		Equals(t, true, acquired)
		Equals(t, pullNum, currLock.Pull.Num)
	}

	t.Log("...succeed if the new project has a different workspace")
	{
		newLock := lock
		newLock.Workspace = "different-workspace"
		acquired, currLock, err := ddb.TryLock(newLock)
		Ok(t, err)
		Equals(t, true, acquired)
		Equals(t, newLock, currLock)
	}

	t.Log("...succeed if the new project has a different repoName")
	{
		newLock := lock
		newLock.Project = models.NewProject("different/repo", project.Path)
		acquired, currLock, err := ddb.TryLock(newLock)
		Ok(t, err)
		Equals(t, true, acquired)
		Equals(t, newLock, currLock)
	}

	t.Log("...not succeed if the new project only has a different pullNum")
	{
		newLock := lock
		newLock.Pull.Num = lock.Pull.Num + 1
		acquired, currLock, err := ddb.TryLock(newLock)
		Ok(t, err)
		Equals(t, false, acquired)
		Equals(t, currLock.Pull.Num, pullNum)
	}
}

func TestUnlockingNoLocks(t *testing.T) {
	t.Log("unlocking with no locks should succeed")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, err := ddb.Unlock(project, workspace)

	Ok(t, err)
}

func TestUnlocking(t *testing.T) {
	t.Log("unlocking with an existing lock should succeed")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)
	_, err = ddb.Unlock(project, workspace)
	Ok(t, err)

	// should be no locks listed
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))

	// should be able to re-lock that repo with a new pull num
	newLock := lock
	newLock.Pull.Num = lock.Pull.Num + 1
	acquired, currLock, err := ddb.TryLock(newLock)
	Ok(t, err)
	Equals(t, true, acquired)
	Equals(t, newLock, currLock)
}

func TestUnlockingMultiple(t *testing.T) {
	t.Log("unlocking and locking multiple locks should succeed")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)

	new := lock
	new.Project.RepoFullName = "new/repo"
	_, _, err = ddb.TryLock(new)
	Ok(t, err)

	new2 := lock
	new2.Project.Path = "new/path"
	_, _, err = ddb.TryLock(new2)
	Ok(t, err)

	new3 := lock
	new3.Workspace = "new-workspace"
	_, _, err = ddb.TryLock(new3)
	Ok(t, err)

	// now try and unlock them
	_, err = ddb.Unlock(new3.Project, new3.Workspace)
	Ok(t, err)
	_, err = ddb.Unlock(new2.Project, workspace)
	Ok(t, err)
	_, err = ddb.Unlock(new.Project, workspace)
	Ok(t, err)
	_, err = ddb.Unlock(project, workspace)
	Ok(t, err)

	// should be none left
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))
}

func TestUnlockByPullNone(t *testing.T) {
	t.Log("UnlockByPull should be successful when there are no locks")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, err := ddb.UnlockByPull("any/repo", 1)
	Ok(t, err)
}

func TestUnlockByPullOne(t *testing.T) {
	t.Log("with one lock, UnlockByPull should...")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)

	t.Log("...delete nothing when its the same repo but a different pull")
	{
		_, err := ddb.UnlockByPull(project.RepoFullName, pullNum+1)
		Ok(t, err)
		ls, err := ddb.List()
		Ok(t, err)
		Equals(t, 1, len(ls))
	}
	t.Log("...delete nothing when its the same pull but a different repo")
	{
		_, err := ddb.UnlockByPull("different/repo", pullNum)
		Ok(t, err)
		ls, err := ddb.List()
		Ok(t, err)
		Equals(t, 1, len(ls))
	}
	t.Log("...delete the lock when its the same repo and pull")
	{
		_, err := ddb.UnlockByPull(project.RepoFullName, pullNum)
		Ok(t, err)
		ls, err := ddb.List()
		Ok(t, err)
		Equals(t, 0, len(ls))
	}
}

func TestUnlockByPullAfterUnlock(t *testing.T) {
	t.Log("after locking and unlocking, UnlockByPull should be successful")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)
	_, err = ddb.Unlock(project, workspace)
	Ok(t, err)

	_, err = ddb.UnlockByPull(project.RepoFullName, pullNum)
	Ok(t, err)
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))
}

func TestUnlockByPullMatching(t *testing.T) {
	t.Log("UnlockByPull should delete all locks in that repo and pull num")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)

	// add additional locks with the same repo and pull num but different paths/workspaces
	new := lock
	new.Project.Path = "dif/path"
	_, _, err = ddb.TryLock(new)
	Ok(t, err)
	new2 := lock
	new2.Workspace = "new-workspace"
	_, _, err = ddb.TryLock(new2)
	Ok(t, err)

	// there should now be 3
	ls, err := ddb.List()
	Ok(t, err)
	Equals(t, 3, len(ls))

	// should all be unlocked
	_, err = ddb.UnlockByPull(project.RepoFullName, pullNum)
	Ok(t, err)
	ls, err = ddb.List()
	Ok(t, err)
	Equals(t, 0, len(ls))
}

func TestGetLockNotThere(t *testing.T) {
	t.Log("getting a lock that doesn't exist should return a nil pointer")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	l, err := ddb.GetLock(project, workspace)
	Ok(t, err)
	Equals(t, (*models.ProjectLock)(nil), l)
}

func TestGetLock(t *testing.T) {
	t.Log("getting a lock should return the lock")
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	_, _, err := ddb.TryLock(lock)
	Ok(t, err)

	l, err := ddb.GetLock(project, workspace)
	Ok(t, err)
	// can't compare against time so doing each field
	Equals(t, lock.Project, l.Project)
	Equals(t, lock.Workspace, l.Workspace)
	Equals(t, lock.Pull, l.Pull)
	Equals(t, lock.User, l.User)
}

// Test we can create a status and then getCommandLock it.
func TestPullStatus_UpdateGet(t *testing.T) {
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	pull := models.PullRequest{
		Num:        1,
		HeadCommit: "sha",
		URL:        "url",
		HeadBranch: "head",
		BaseBranch: "base",
		Author:     "lkysow",
		State:      models.OpenPullState,
		BaseRepo: models.Repo{
			FullName:          "runatlantis/atlantis",
			Owner:             "runatlantis",
			Name:              "atlantis",
			CloneURL:          "clone-url",
			SanitizedCloneURL: "clone-url",
			VCSHost: models.VCSHost{
				Hostname: "github.com",
				Type:     models.Github,
			},
		},
	}
	status, err := ddb.UpdatePullWithResults(
		pull,
		[]command.ProjectResult{
			{
				Command:    command.Plan,
				RepoRelDir: ".",
				Workspace:  "default",
				Failure:    "failure",
			},
		})
	Ok(t, err)

	maybeStatus, err := ddb.GetPullStatus(pull)
	Ok(t, err)
	Equals(t, pull, maybeStatus.Pull) // nolint: staticcheck
	Equals(t, []models.ProjectStatus{
		{
			Workspace:   "default",
			RepoRelDir:  ".",
			ProjectName: "",
			Status:      models.ErroredPlanStatus,
		},
	}, status.Projects)
}

// Test we can create a status, delete it, and then we shouldn't be able to getCommandLock
// it.
func TestPullStatus_UpdateDeleteGet(t *testing.T) {
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	pull := models.PullRequest{
		Num:        1,
		HeadCommit: "sha",
		URL:        "url",
		HeadBranch: "head",
		BaseBranch: "base",
		Author:     "lkysow",
		State:      models.OpenPullState,
		BaseRepo: models.Repo{
			FullName:          "runatlantis/atlantis",
			Owner:             "runatlantis",
			Name:              "atlantis",
			CloneURL:          "clone-url",
			SanitizedCloneURL: "clone-url",
			VCSHost: models.VCSHost{
				Hostname: "github.com",
				Type:     models.Github,
			},
		},
	}
	_, err := ddb.UpdatePullWithResults(
		pull,
		[]command.ProjectResult{
			{
				RepoRelDir: ".",
				Workspace:  "default",
				Failure:    "failure",
			},
		})
	Ok(t, err)

	err = ddb.DeletePullStatus(pull)
	Ok(t, err)

	maybeStatus, err := ddb.GetPullStatus(pull)
	Ok(t, err)
	Assert(t, maybeStatus == nil, "exp nil")
}

// Test we can create a status, update a specific project's status within that
// pull status, and when we getCommandLock all the project statuses, that specific project
// should be updated.
func TestPullStatus_UpdateProject(t *testing.T) {
	ddb := newTestDynamoDB()
	createDynamoDBTable(t, ddb)
	defer deleteDynamoDBTable(t, ddb)

	pull := models.PullRequest{
		Num:        1,
		HeadCommit: "sha",
		URL:        "url",
		HeadBranch: "head",
		BaseBranch: "base",
		Author:     "lkysow",
		State:      models.OpenPullState,
		BaseRepo: models.Repo{
			FullName:          "runatlantis/atlantis",
			Owner:             "runatlantis",
			Name:              "atlantis",
			CloneURL:          "clone-url",
			SanitizedCloneURL: "clone-url",
			VCSHost: models.VCSHost{
				Hostname: "github.com",
				Type:     models.Github,
			},
		},
	}
	_, err := ddb.UpdatePullWithResults(
		pull,
		[]command.ProjectResult{
			{
				RepoRelDir: ".",
				Workspace:  "default",
				Failure:    "failure",
			},
			{
				RepoRelDir:   ".",
				Workspace:    "staging",
				ApplySuccess: "success!",
			},
		})
	Ok(t, err)

	err = ddb.UpdateProjectStatus(pull, "default", ".", models.DiscardedPlanStatus)
	Ok(t, err)

	status, err := ddb.GetPullStatus(pull)
	Ok(t, err)
	Equals(t, pull, status.Pull) // nolint: staticcheck
	Equals(t, []models.ProjectStatus{
		{
			Workspace:   "default",
			RepoRelDir:  ".",
			ProjectName: "",
			Status:      models.DiscardedPlanStatus,
		},
		{
			Workspace:   "staging",
			RepoRelDir:  ".",
			ProjectName: "",
			Status:      models.AppliedPlanStatus,
		},
	}, status.Projects) // nolint: staticcheck
}

// Test that if we update an existing pull status and our new status is for a
// different HeadSHA, that we just overwrite the old status.
func TestPullStatus_UpdateNewCommit(t *testing.T) {
	ddb := newTestDynamoDB()

	pull := models.PullRequest{
		Num:        1,
		HeadCommit: "sha",
		URL:        "url",
		HeadBranch: "head",
		BaseBranch: "base",
		Author:     "lkysow",
		State:      models.OpenPullState,
		BaseRepo: models.Repo{
			FullName:          "runatlantis/atlantis",
			Owner:             "runatlantis",
			Name:              "atlantis",
			CloneURL:          "clone-url",
			SanitizedCloneURL: "clone-url",
			VCSHost: models.VCSHost{
				Hostname: "github.com",
				Type:     models.Github,
			},
		},
	}
	_, err := ddb.UpdatePullWithResults(
		pull,
		[]command.ProjectResult{
			{
				RepoRelDir: ".",
				Workspace:  "default",
				Failure:    "failure",
			},
		})
	Ok(t, err)

	pull.HeadCommit = "newsha"
	status, err := ddb.UpdatePullWithResults(pull,
		[]command.ProjectResult{
			{
				RepoRelDir:   ".",
				Workspace:    "staging",
				ApplySuccess: "success!",
			},
		})

	Ok(t, err)
	Equals(t, 1, len(status.Projects))

	maybeStatus, err := ddb.GetPullStatus(pull)
	Ok(t, err)
	Equals(t, pull, maybeStatus.Pull)
	Equals(t, []models.ProjectStatus{
		{
			Workspace:   "staging",
			RepoRelDir:  ".",
			ProjectName: "",
			Status:      models.AppliedPlanStatus,
		},
	}, maybeStatus.Projects)
}

// Test that if we update an existing pull status and our new status is for a
// the same commit, that we merge the statuses.
func TestPullStatus_UpdateMerge(t *testing.T) {
	ddb := newTestDynamoDB()

	pull := models.PullRequest{
		Num:        1,
		HeadCommit: "sha",
		URL:        "url",
		HeadBranch: "head",
		BaseBranch: "base",
		Author:     "lkysow",
		State:      models.OpenPullState,
		BaseRepo: models.Repo{
			FullName:          "runatlantis/atlantis",
			Owner:             "runatlantis",
			Name:              "atlantis",
			CloneURL:          "clone-url",
			SanitizedCloneURL: "clone-url",
			VCSHost: models.VCSHost{
				Hostname: "github.com",
				Type:     models.Github,
			},
		},
	}
	_, err := ddb.UpdatePullWithResults(
		pull,
		[]command.ProjectResult{
			{
				Command:    command.Plan,
				RepoRelDir: "mergeme",
				Workspace:  "default",
				Failure:    "failure",
			},
			{
				Command:     command.Plan,
				RepoRelDir:  "projectname",
				Workspace:   "default",
				ProjectName: "projectname",
				Failure:     "failure",
			},
			{
				Command:    command.Plan,
				RepoRelDir: "staythesame",
				Workspace:  "default",
				PlanSuccess: &models.PlanSuccess{
					TerraformOutput: "tf out",
					LockURL:         "lock-url",
					RePlanCmd:       "plan command",
					ApplyCmd:        "apply command",
				},
			},
		})
	Ok(t, err)

	updateStatus, err := ddb.UpdatePullWithResults(pull,
		[]command.ProjectResult{
			{
				Command:      command.Apply,
				RepoRelDir:   "mergeme",
				Workspace:    "default",
				ApplySuccess: "applied!",
			},
			{
				Command:     command.Apply,
				RepoRelDir:  "projectname",
				Workspace:   "default",
				ProjectName: "projectname",
				Error:       errors.New("apply error"),
			},
			{
				Command:      command.Apply,
				RepoRelDir:   "newresult",
				Workspace:    "default",
				ApplySuccess: "success!",
			},
		})
	Ok(t, err)

	getStatus, err := ddb.GetPullStatus(pull)
	Ok(t, err)

	// Test both the pull state returned from the update call *and* the getCommandLock
	// call.
	for _, s := range []models.PullStatus{updateStatus, *getStatus} {
		Equals(t, pull, s.Pull)
		Equals(t, []models.ProjectStatus{
			{
				RepoRelDir: "mergeme",
				Workspace:  "default",
				Status:     models.AppliedPlanStatus,
			},
			{
				RepoRelDir:  "projectname",
				Workspace:   "default",
				ProjectName: "projectname",
				Status:      models.ErroredApplyStatus,
			},
			{
				RepoRelDir: "staythesame",
				Workspace:  "default",
				Status:     models.PlannedPlanStatus,
			},
			{
				RepoRelDir: "newresult",
				Workspace:  "default",
				Status:     models.AppliedPlanStatus,
			},
		}, updateStatus.Projects)
	}
}

func newTestDynamoDB() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           "dev",
		Config: aws.Config{
			Region: aws.String("eu-west-1"),
		},
	}))

	ddb, err := dynamodb.NewWithClient(dynamodb_aws.New(sess), "atlantis_lock_tests")

	if err != nil {
		panic(errors.Wrap(err, "failed to create test dynamodb client"))
	}

	return ddb
}

func createDynamoDBTable(t *testing.T, dynClient *dynamodb.DynamoDB) {
	createInput := &dynamodb_aws.CreateTableInput{
		AttributeDefinitions: []*dynamodb_aws.AttributeDefinition{
			{
				AttributeName: aws.String("LockID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb_aws.KeySchemaElement{
			{
				AttributeName: aws.String("LockID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb_aws.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(dynClient.Table),
	}

	_, err := dynClient.Client.CreateTable(createInput)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	time.Sleep(time.Second)

	describeInput := &dynamodb_aws.DescribeTableInput{
		TableName: aws.String(dynClient.Table),
	}

	for {
		resp, err := dynClient.Client.DescribeTable(describeInput)
		if err != nil {
			t.Fatal(err)
		}

		if *resp.Table.TableStatus == "ACTIVE" {
			return
		}

		if time.Since(start) > time.Minute {
			t.Fatalf("timed out creating DynamoDB table %s", dynClient.Table)
		}

		time.Sleep(3 * time.Second)
	}

}

func deleteDynamoDBTable(t *testing.T, dynClient *dynamodb.DynamoDB) {
	params := &dynamodb_aws.DeleteTableInput{
		TableName: aws.String(dynClient.Table),
	}
	_, err := dynClient.Client.DeleteTable(params)
	if err != nil {
		t.Logf("WARNING: Failed to delete the test DynamoDB table %q. It has been left in your AWS account and may incur charges. (error was %s)", dynClient.Table, err)
	}
}

package s3

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"

	"github.com/omeid/gonzo/context"
	"github.com/omeid/gonzo"
)

type ACL s3.ACL

const (
	Private           = ACL("private")
	PublicRead        = ACL("public-read")
	PublicReadWrite   = ACL("public-read-write")
	AuthenticatedRead = ACL("authenticated-read")
	BucketOwnerRead   = ACL("bucket-owner-read")
	BucketOwnerFull   = ACL("bucket-owner-full-control")
)

type Region aws.Region

var (
	APNortheast  = Region(aws.APNortheast)
	APSoutheast  = Region(aws.APSoutheast)
	APSoutheast2 = Region(aws.APSoutheast2)
	EUWest       = Region(aws.EUWest)
	USEast       = Region(aws.USEast)
	USWest       = Region(aws.USWest)
	USWest2      = Region(aws.USWest2)
	SAEast       = Region(aws.SAEast)
	CNNorth      = Region(aws.CNNorth)
)

type Config struct {
	AccessKey string
	SecretKey string
	Region    Region
	Name      string
	Perm      ACL
}

func checkconfig(c Config) error {

	missing := []string{}

	if c.AccessKey == "" {
		missing = append(missing, "AccessKey")
	}
	if c.SecretKey == "" {
		missing = append(missing, "SecretKey")
	}
	if c.Region.Name == "" {
		missing = append(missing, "Region")
	}
	if c.Name == "" {
		missing = append(missing, "Name")
	}
	if c.Perm == "" {
		missing = append(missing, "Perm")
	}

	if len(missing) != 0 {
		return fmt.Errorf("Missing Required Field %s ", strings.Join(missing, ", "))
	}
	return nil
}

func Put(c Config) gonzo.Stage {
	return func(ctx context.Context, files <-chan gonzo.File, out chan<- gonzo.File) error {

		err := checkconfig(c)
		if err != nil {
			return err
		}

		auth := aws.Auth{
			AccessKey: c.AccessKey,
			SecretKey: c.SecretKey,
		}

		con := s3.New(auth, aws.Region(c.Region))
		bucket := con.Bucket(c.Name)

		for {
			select {
			case file, ok := <-files:
				if !ok {
					return nil
				}
				if file.FileInfo().IsDir() {
					continue
				}

				content, err := ioutil.ReadAll(file)
				if err != nil {
					return err
				}

				name := file.FileInfo().Name()

				contentType := mime.TypeByExtension(filepath.Ext(name))
				if contentType == "" {
					contentType = http.DetectContentType(content)
				}
				ctx = context.WithValue(ctx, "Content-Type", contentType)
				ctx.Infof("Uploading %s", name)

				err = bucket.Put(name, content, contentType, s3.ACL(c.Perm))
				if err != nil {
					return err
				}

				out <- gonzo.NewFile(ioutil.NopCloser(bytes.NewReader(content)), file.FileInfo())
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

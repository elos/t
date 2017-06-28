package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"google.golang.org/grpc"

	"github.com/elos/x/auth"
	"github.com/elos/x/data"
	"github.com/elos/x/models"
)

const (
	dbaddrKey  = "ELOS_DB_ADDR"
	publicKey  = "ELOS_PUBLIC_CRED"
	privateKey = "ELOS_PRIVATE_CRED"
)

var (
	dbaddr  = "localhost:4444"
	public  = "p"
	private = "p"
)

func init() {
	if s, ok := os.LookupEnv(dbaddrKey); ok {
		dbaddr = s
	}

	if s, ok := os.LookupEnv(publicKey); ok {
		public = s
	}

	if s, ok := os.LookupEnv(privateKey); ok {
		private = s
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch len(os.Args) {
	case 1:
		ls(ctx)
	default:
		switch cmd := os.Args[1]; cmd {
		case "ls":
			ls(ctx)
		case "mk":
			mkflags.Parse(os.Args[2:])
			mk(ctx)
		case "rm":
			rmflags.Parse(os.Args[2:])
			rm(ctx)
		default:
			log.Fatalf("command unrecognized: %q", cmd)
		}
	}
}

func client() (data.DBClient, error) {
	conn, err := grpc.Dial(dbaddr, grpc.WithInsecure(), grpc.WithPerRPCCredentials(auth.RawCredentials(public, private)))
	if err != nil {
		return nil, err
	}

	return data.NewDBClient(conn), nil
}

func ls(ctx context.Context) {
	dbc, err := client()
	if err != nil {
		log.Fatal(err)
	}

	stream, err := dbc.Query(ctx, &data.Query{
		Kind:   models.Kind_TASK,
		Orders: []string{"name"},
	})

	for {
		rec, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Fatal(err)
		}

		log.Println(rec.Task)
	}
}

var mkflags = flag.NewFlagSet("mk", flag.ExitOnError)
var (
	name = mkflags.String("name", "", "name of task")
)

func mk(ctx context.Context) {
	dbc, err := client()
	if err != nil {
		log.Fatal(err)
	}

	rec, err := dbc.Mutate(ctx, &data.Mutation{
		Op: data.Mutation_CREATE,
		Record: &data.Record{
			Kind: models.Kind_TASK,
			Task: &models.Task{
				Name: *name,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("created: %v", rec)
}

var rmflags = flag.NewFlagSet("rm", flag.ExitOnError)
var (
	id = rmflags.String("id", "", "id of task")
)

func rm(ctx context.Context) {
	dbc, err := client()
	if err != nil {
		log.Fatal(err)
	}

	for _, id := range strings.Split(*id, ",") {
		_, err = dbc.Mutate(ctx, &data.Mutation{
			Op: data.Mutation_DELETE,
			Record: &data.Record{
				Kind: models.Kind_TASK,
				Task: &models.Task{Id: id},
			},
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

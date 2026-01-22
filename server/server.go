package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "github.com/marcelloh/fastdb/user"

	"github.com/marcelloh/fastdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedUserServiceServer
	db *fastdb.DB
}

const (
	bucket   = "user"
	dbPath   = "users.db"
	buntPath = "buntData.db"
	syncTime = 100
)

type User struct {
	CreatedAt string `json:"CreatedAt"`
	UUID      string `json:"UUID"`
	Email     string `json:"Email"`
	Password  string `json:"Password"`
	Image     string `json:"Image"`
	ID        int    `json:"ID"`
	IsAdmin   bool   `json:"IsAdmin"`
}

func loadFromBunt(db *fastdb.DB, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	lines := strings.Split(string(content), "\n")
	i := 0
	for i < len(lines) {
		line := strings.TrimSpace(lines[i])
		if line == "*3" {
			i += 3
			i += 1
			key := strings.TrimSpace(lines[i])
			i += 1
			i += 1
			value := strings.TrimSpace(lines[i])
			i += 1

			if after, ok := strings.CutPrefix(key, "user_"); ok {
				idStr := after
				id, err := strconv.Atoi(idStr)
				if err != nil {
					continue
				}

				var u User
				if err := json.Unmarshal([]byte(value), &u); err != nil {
					continue
				}
				if u.ID != id {
					continue
				}
				db.Set(bucket, id, []byte(value))
			}
		} else {
			i += 1
		}
	}
	return nil
}

func validateUser(u *pb.UserDTO) error {
	if u == nil {
		return errors.New("User data required")
	}
	if u.Email == "" {
		return errors.New("Email required")
	}
	var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(u.Email) {
		return errors.New("Invalid Email format")
	}
	if u.Password == "" {
		return errors.New("Password required")
	}
	if u.UUID == "" {
		return errors.New("UUID required")
	}
	_, err := time.Parse(time.RFC3339, u.CreatedAt)
	if err != nil {
		return errors.New("Invalid CreatedAt format")
	}
	return nil
}

func pbUserToUser(pbUser *pb.UserDTO, id int) *User {
	return &User{
		CreatedAt: pbUser.CreatedAt,
		UUID:      pbUser.UUID,
		Email:     pbUser.Email,
		Password:  pbUser.Password,
		Image:     pbUser.Image,
		ID:        id,
		IsAdmin:   pbUser.IsAdmin,
	}
}

func LoadData() *server {
	db, err := fastdb.Open(dbPath, syncTime)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}

	all, err := db.GetAll(bucket)
	if len(all) == 0 {
		if err := loadFromBunt(db, buntPath); err != nil {
			log.Printf("Warning: Failed to load from buntData.db: %v", err)
		}
	}

	return &server{db: db}
}

func (s *server) Get(ctx context.Context, req *pb.IDRequest) (*pb.User, error) {
	if req.ID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "ID must be positive")
	}

	data, ok := s.db.Get(bucket, int(req.ID))
	if !ok {
		return nil, status.Error(codes.NotFound, "User not found")
	}
	var u User
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, status.Error(codes.Internal, "Invalid data")
	}
	return &pb.User{
		CreatedAt: u.CreatedAt,
		UUID:      u.UUID,
		Email:     u.Email,
		Password:  u.Password,
		Image:     u.Image,
		ID:        int32(u.ID),
		IsAdmin:   u.IsAdmin,
	}, nil
}

func (s *server) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	allData, err := s.db.GetAll(bucket)
	if err != nil {
		return nil, status.Error(codes.NotFound, "Bucket not found")
	}

	var ids []int
	var userMap = make(map[int][]byte)

	for id, data := range allData {
		ids = append(ids, id)
		userMap[id] = data
	}

	sort.Ints(ids)

	total := len(ids)

	offset := int(req.Offset)

	if req.Limit < 0 || req.Limit > 50000 {
		return nil, status.Error(codes.InvalidArgument, "Limit must be in range [0, 50000]")
	}
	limit := int(req.Limit)

	if offset >= total {
		return &pb.GetAllResponse{
			Users:  []*pb.User{},
			Total:  int32(total),
			Offset: int32(offset),
			Limit:  int32(limit),
		}, nil
	}

	end := min(offset+limit, total)

	var users []*pb.User
	for i := offset; i < end; i++ {
		id := ids[i]
		data := userMap[id]

		var u User
		if err := json.Unmarshal(data, &u); err != nil {
			continue
		}

		users = append(users, &pb.User{
			CreatedAt: u.CreatedAt,
			UUID:      u.UUID,
			Email:     u.Email,
			Password:  u.Password,
			Image:     u.Image,
			ID:        int32(u.ID),
			IsAdmin:   u.IsAdmin,
		})
	}

	return &pb.GetAllResponse{
		Users:  users,
		Total:  int32(total),
		Offset: int32(offset),
		Limit:  int32(limit),
	}, nil
}

func (s *server) Count(ctx context.Context, req *pb.EmptyRequest) (*pb.CountResponse, error) {
	all, err := s.db.GetAll(bucket)
	if err != nil {
		return &pb.CountResponse{Count: 0}, nil
	}
	return &pb.CountResponse{Count: int32(len(all))}, nil
}

func (s *server) Insert(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if req.ID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "ID must be positive")
	}

	if req.Data == nil {
		return nil, status.Error(codes.InvalidArgument, "User data required")
	}

	if err := validateUser(req.Data); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	_, ok := s.db.Get(bucket, int(req.ID))
	if ok {
		return nil, status.Error(codes.AlreadyExists, "User already exists")
	}

	u := pbUserToUser(req.Data, int(req.ID))
	userJSON, err := json.Marshal(u)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = s.db.Set(bucket, u.ID, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if req.ID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "ID must be positive")
	}

	if req.Data == nil {
		return nil, status.Error(codes.InvalidArgument, "User data required")
	}

	if err := validateUser(req.Data); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	_, exists := s.db.Get(bucket, int(req.ID))
	if !exists {
		return nil, status.Error(codes.NotFound, "User not found for update")
	}

	u := pbUserToUser(req.Data, int(req.ID))
	userJSON, err := json.Marshal(u)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	err = s.db.Set(bucket, u.ID, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.SuccessResponse{Success: true}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.IDRequest) (*pb.SuccessResponse, error) {
	ok, err := s.db.Del(bucket, int(req.ID))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "User not found")
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterUserServiceServer(grpcServer, LoadData())
	log.Println("Server is running on localhost:3000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/marcelloh/fastdb/user"

	"github.com/marcelloh/fastdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	bucket   = "user"
	buntPath = "buntData.db"
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

type UserServer struct {
	pb.UnimplementedUserServiceServer
	pb.UnimplementedElectionServiceServer

	config              *ServerConfig
	processedRequests   map[string]bool
	reqMu               sync.RWMutex
	mu                  sync.RWMutex
	isLeader            bool
	currentLeader       int
	electionTriggered   bool
	electionTriggeredMu sync.Mutex
}

func NewUserServer(config *ServerConfig) *UserServer {
	all, _ := config.db.GetAll(bucket)
	if len(all) == 0 {
		_ = loadFromBunt(config.db, buntPath)
	}

	return &UserServer{
		config:            config,
		processedRequests: make(map[string]bool),
		currentLeader:     config.peers[0],
		isLeader:          config.myID == config.peers[0],
		electionTriggered: false,
	}
}

func validateUser(u *pb.UserDTO) error {
	if u == nil {
		return status.Error(codes.InvalidArgument, "User data required")
	}
	if u.Email == "" {
		return status.Error(codes.InvalidArgument, "Email required")
	}

	var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if !emailRegex.MatchString(u.Email) {
		return status.Error(codes.InvalidArgument, "Invalid Email format")
	}

	if u.Password == "" {
		return status.Error(codes.InvalidArgument, "Password required")
	}
	if u.UUID == "" {
		return status.Error(codes.InvalidArgument, "UUID required")
	}

	if u.CreatedAt != "" {
		_, err := time.Parse(time.RFC3339, u.CreatedAt)
		if err != nil {
			return status.Error(codes.InvalidArgument, "Invalid CreatedAt format")
		}
	}

	return nil
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

func (s *UserServer) checkIsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isLeader
}

func (s *UserServer) setLeader(leaderID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentLeader = leaderID
	s.isLeader = (leaderID == s.config.myID)
	log.Printf("%s [Server %d] Server %d became Primary", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, leaderID)

	s.electionTriggeredMu.Lock()
	s.electionTriggered = false
	s.electionTriggeredMu.Unlock()
}

func (s *UserServer) hasProcessed(reqID string) bool {
	s.reqMu.RLock()
	_, ok := s.processedRequests[reqID]
	s.reqMu.RUnlock()
	return ok
}

func (s *UserServer) markProcessed(reqID string) {
	s.reqMu.Lock()
	s.processedRequests[reqID] = true
	s.reqMu.Unlock()
}

func (s *UserServer) localInsert(req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if err := validateUser(req.Data); err != nil {
		return nil, err
	}

	id := int(req.ID)
	_, exists := s.config.db.Get(bucket, id)
	if exists {
		return nil, status.Error(codes.AlreadyExists, "User already exists")
	}

	u := &User{
		CreatedAt: req.Data.CreatedAt,
		UUID:      req.Data.UUID,
		Email:     req.Data.Email,
		Password:  req.Data.Password,
		Image:     req.Data.Image,
		ID:        id,
		IsAdmin:   req.Data.IsAdmin,
	}
	userJSON, _ := json.Marshal(u)

	err := s.config.db.Set(bucket, id, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) localSet(req *pb.SetRequest) (*pb.SuccessResponse, error) {
	if err := validateUser(req.Data); err != nil {
		return nil, err
	}

	id := int(req.ID)
	_, exists := s.config.db.Get(bucket, id)
	if !exists {
		return nil, status.Error(codes.NotFound, "User not found")
	}

	u := &User{
		CreatedAt: req.Data.CreatedAt,
		UUID:      req.Data.UUID,
		Email:     req.Data.Email,
		Password:  req.Data.Password,
		Image:     req.Data.Image,
		ID:        id,
		IsAdmin:   req.Data.IsAdmin,
	}
	userJSON, _ := json.Marshal(u)

	err := s.config.db.Set(bucket, id, userJSON)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) localDelete(req *pb.IDRequest) (*pb.SuccessResponse, error) {
	id := int(req.ID)
	_, exists := s.config.db.Get(bucket, id)
	if !exists {
		return &pb.SuccessResponse{Success: true}, nil
	}

	_, err := s.config.db.Del(bucket, id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SuccessResponse{Success: true}, nil
}

func (s *UserServer) handleInsert(req *pb.SetRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}

	source := "Client"
	if !isFromClient {
		source = "Primary (replication)"
	}
	log.Printf("%s [Server %d] [%s] Received Insert request from %s", now, s.config.myID, role, source)

	if isFromClient {
		if !s.checkIsLeader() {
			s.electionTriggeredMu.Lock()
			if !s.electionTriggered {
				s.electionTriggered = true
				go s.initiateElection()
				log.Printf("%s [Server %d] [Backup] Not leader, triggering election due to client write request", now, s.config.myID)
			}
			s.electionTriggeredMu.Unlock()

			return nil, status.Error(codes.Unavailable, "Cannot connect this server")
		}
	} else {
		if s.checkIsLeader() {
			return nil, status.Error(codes.PermissionDenied, "Leader does not accept replication")
		}
	}

	if req.RequestId == "" {
		req.RequestId = uuid.New().String()
	}
	if s.hasProcessed(req.RequestId) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localInsert(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Insert")
	}

	s.markProcessed(req.RequestId)
	return resp, nil
}

func (s *UserServer) handleSet(req *pb.SetRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}

	source := "Client"
	if !isFromClient {
		source = "Primary (replication)"
	}
	log.Printf("%s [Server %d] [%s] Received Set request from %s", now, s.config.myID, role, source)

	if isFromClient {
		if !s.checkIsLeader() {
			s.electionTriggeredMu.Lock()
			if !s.electionTriggered {
				s.electionTriggered = true
				go s.initiateElection()
				log.Printf("%s [Server %d] [Backup] Not leader, triggering election due to client write request", now, s.config.myID)
			}
			s.electionTriggeredMu.Unlock()

			return nil, status.Error(codes.Unavailable, "Cannot connect this server")
		}
	} else {
		if s.checkIsLeader() {
			return nil, status.Error(codes.PermissionDenied, "Leader does not accept replication")
		}
	}

	if req.RequestId == "" {
		req.RequestId = uuid.New().String()
	}
	if s.hasProcessed(req.RequestId) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localSet(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Set")
	}

	s.markProcessed(req.RequestId)
	return resp, nil
}

func (s *UserServer) handleDelete(req *pb.IDRequest, isFromClient bool) (*pb.SuccessResponse, error) {
	now := time.Now().Format("2006-01-02 15:04:05")
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}

	source := "Client"
	if !isFromClient {
		source = "Primary (replication)"
	}
	log.Printf("%s [Server %d] [%s] Received Delete request from %s", now, s.config.myID, role, source)

	if isFromClient {
		if !s.checkIsLeader() {
			s.electionTriggeredMu.Lock()
			if !s.electionTriggered {
				s.electionTriggered = true
				go s.initiateElection()
				log.Printf("%s [Server %d] [Backup] Not leader, triggering election due to client write request", now, s.config.myID)
			}
			s.electionTriggeredMu.Unlock()

			return nil, status.Error(codes.Unavailable, "Cannot connect this server")
		}
	} else {
		if s.checkIsLeader() {
			return nil, status.Error(codes.PermissionDenied, "Leader does not accept replication")
		}
	}

	if req.RequestID == "" {
		req.RequestID = uuid.New().String()
	}
	if s.hasProcessed(req.RequestID) {
		return &pb.SuccessResponse{Success: true}, nil
	}

	resp, err := s.localDelete(req)
	if err != nil {
		return resp, err
	}

	if isFromClient {
		go s.replicate(req, "Delete")
	}

	s.markProcessed(req.RequestID)
	return resp, nil
}

func (s *UserServer) Insert(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleInsert(req, true)
}

func (s *UserServer) ReplicateInsert(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleInsert(req, false)
}

func (s *UserServer) Set(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleSet(req, true)
}

func (s *UserServer) ReplicateSet(ctx context.Context, req *pb.SetRequest) (*pb.SuccessResponse, error) {
	return s.handleSet(req, false)
}

func (s *UserServer) Delete(ctx context.Context, req *pb.IDRequest) (*pb.SuccessResponse, error) {
	return s.handleDelete(req, true)
}

func (s *UserServer) ReplicateDelete(ctx context.Context, req *pb.IDRequest) (*pb.SuccessResponse, error) {
	return s.handleDelete(req, false)
}

func (s *UserServer) Get(ctx context.Context, req *pb.IDRequest) (*pb.User, error) {
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}
	log.Printf("%s [Server %d] [%s] Received Get request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, role)

	data, ok := s.config.db.Get(bucket, int(req.ID))
	if !ok {
		return nil, status.Error(codes.NotFound, "User not found")
	}

	var u User
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, status.Error(codes.Internal, "Failed to parse user data")
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

func (s *UserServer) GetAll(ctx context.Context, req *pb.GetAllRequest) (*pb.GetAllResponse, error) {
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}
	log.Printf("%s [Server %d] [%s] Received GetAll request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, role)

	allData, err := s.config.db.GetAll(bucket)
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
	limit := max(int(req.Limit), 50)

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

func (s *UserServer) Count(ctx context.Context, req *pb.EmptyRequest) (*pb.CountResponse, error) {
	role := "Backup"
	if s.checkIsLeader() {
		role = "Primary"
	}
	log.Printf("%s [Server %d] [%s] Received Count request from Client", time.Now().Format("2006-01-02 15:04:05"), s.config.myID, role)

	all, err := s.config.db.GetAll(bucket)
	if err != nil {
		return &pb.CountResponse{Count: 0}, nil
	}

	count := len(all)
	return &pb.CountResponse{Count: int32(count)}, nil
}

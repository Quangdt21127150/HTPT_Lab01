package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	pb "github.com/marcelloh/fastdb/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

type Client struct {
	conn       *grpc.ClientConn
	client     pb.UserServiceClient
	election   pb.ElectionServiceClient
	leaderAddr string
}

const (
	maxRetries = 5
	baseDelay  = 300 * time.Millisecond
)

var peersList = []string{"localhost:3001", "localhost:3002", "localhost:3003"}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:       conn,
		client:     pb.NewUserServiceClient(conn),
		election:   pb.NewElectionServiceClient(conn),
		leaderAddr: addr,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) SwitchLeader(newAddr string) error {
	c.conn.Close()
	conn, err := grpc.NewClient(newAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c.conn = conn
	c.client = pb.NewUserServiceClient(conn)
	c.election = pb.NewElectionServiceClient(conn)
	c.leaderAddr = newAddr
	log.Printf("Switched to new leader: %s", newAddr)
	return nil
}

func (c *Client) DiscoverLeader(peers []string) (string, error) {
	for _, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Cannot connect to %s: %v", addr, err)
			continue
		}
		client := pb.NewElectionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.GetLeader(ctx, &pb.EmptyRequest{})
		cancel()
		conn.Close()
		if err == nil {
			leaderID := int(resp.ID)
			leaderAddr := fmt.Sprintf("localhost:%d", 3000+leaderID)
			log.Printf("Discovered leader: %d at %s", leaderID, leaderAddr)
			return leaderAddr, nil
		}
	}
	return "", fmt.Errorf("cannot discover leader from any peer")
}

func (c *Client) retryWrite(fn func() (bool, error)) (bool, error) {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		success, err := fn()
		if err == nil {
			return success, nil
		}
		lastErr = err

		st, ok := status.FromError(err)
		if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.DeadlineExceeded) {
			return false, err
		}

		newLeader, discErr := c.DiscoverLeader(peersList)
		if discErr == nil && newLeader != c.leaderAddr {
			_ = c.SwitchLeader(newLeader)
		}

		delay := baseDelay * time.Duration(attempt)
		jitter := time.Duration(rand.Intn(100)) * time.Millisecond
		time.Sleep(delay + jitter)
	}
	return false, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

func (c *Client) Get(id int32) (*User, error) {
	resp, err := c.client.Get(context.Background(), &pb.IDRequest{ID: id})
	if err != nil {
		return nil, err
	}
	return &User{
		CreatedAt: resp.CreatedAt,
		UUID:      resp.UUID,
		Email:     resp.Email,
		Password:  resp.Password,
		Image:     resp.Image,
		ID:        int(resp.ID),
		IsAdmin:   resp.IsAdmin,
	}, nil
}

func (c *Client) GetAll(offset, limit int32) ([]*User, error) {
	resp, err := c.client.GetAll(context.Background(), &pb.GetAllRequest{Offset: offset, Limit: limit})
	if err != nil {
		return nil, err
	}
	users := make([]*User, len(resp.Users))
	for i, u := range resp.Users {
		users[i] = &User{
			CreatedAt: u.CreatedAt,
			UUID:      u.UUID,
			Email:     u.Email,
			Password:  u.Password,
			Image:     u.Image,
			ID:        int(u.ID),
			IsAdmin:   u.IsAdmin,
		}
	}
	return users, nil
}

func (c *Client) Count() (int32, error) {
	resp, err := c.client.Count(context.Background(), &pb.EmptyRequest{})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *Client) Insert(user *User, requestID string) (bool, error) {
	return c.retryWrite(func() (bool, error) {
		if requestID == "" {
			requestID = uuid.New().String()
			log.Printf("Generated RequestId for Insert: %s", requestID)
		}
		resp, err := c.client.Insert(context.Background(), &pb.SetRequest{
			ID: int32(user.ID),
			Data: &pb.UserDTO{
				CreatedAt: user.CreatedAt,
				UUID:      user.UUID,
				Email:     user.Email,
				Password:  user.Password,
				Image:     user.Image,
				IsAdmin:   user.IsAdmin,
			},
			RequestId: requestID,
		})
		if err != nil {
			return false, err
		}
		return resp.Success, nil
	})
}

func (c *Client) Set(user *User, requestID string) (bool, error) {
	return c.retryWrite(func() (bool, error) {
		if requestID == "" {
			requestID = uuid.New().String()
			log.Printf("Generated RequestId for Set: %s", requestID)
		}
		resp, err := c.client.Set(context.Background(), &pb.SetRequest{
			ID: int32(user.ID),
			Data: &pb.UserDTO{
				CreatedAt: user.CreatedAt,
				UUID:      user.UUID,
				Email:     user.Email,
				Password:  user.Password,
				Image:     user.Image,
				IsAdmin:   user.IsAdmin,
			},
			RequestId: requestID,
		})
		if err != nil {
			return false, err
		}
		return resp.Success, nil
	})
}

func (c *Client) Delete(id int32, requestID string) (bool, error) {
	return c.retryWrite(func() (bool, error) {
		if requestID == "" {
			requestID = uuid.New().String()
			log.Printf("Generated RequestId for Delete: %s", requestID)
		}
		resp, err := c.client.Delete(context.Background(), &pb.IDRequest{
			ID:        id,
			RequestID: requestID,
		})
		if err != nil {
			return false, err
		}
		return resp.Success, nil
	})
}

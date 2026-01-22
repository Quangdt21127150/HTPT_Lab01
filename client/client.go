package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/marcelloh/fastdb/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

type ListOptions struct {
	Offset int32 `json:"offset,omitempty"`
	Limit  int32 `json:"limit,omitempty"`
}

type Client struct {
	conn   *grpc.ClientConn
	client pb.UserServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		client: pb.NewUserServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
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
		ID:        int(id),
		IsAdmin:   resp.IsAdmin,
	}, nil
}

func (c *Client) GetAll(opts ...int32) ([]*User, error) {
	var opt ListOptions
	if len(opts) > 0 {
		opt.Limit = opts[0]
	}
	if len(opts) == 2 {
		opt.Offset = opts[0]
		opt.Limit = opts[1]
	}

	resp, err := c.client.GetAll(context.Background(), &pb.GetAllRequest{Offset: opt.Offset, Limit: opt.Limit})
	if err != nil {
		return nil, err
	}
	var users []*User
	for _, pbUser := range resp.Users {
		users = append(users, &User{
			CreatedAt: pbUser.CreatedAt,
			UUID:      pbUser.UUID,
			Email:     pbUser.Email,
			Password:  pbUser.Password,
			Image:     pbUser.Image,
			ID:        int(pbUser.ID),
			IsAdmin:   pbUser.IsAdmin,
		})
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

func (c *Client) Insert(user *User) (bool, error) {
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
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *Client) Set(user *User) (bool, error) {
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
	})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func (c *Client) Delete(id int32) (bool, error) {
	resp, err := c.client.Delete(context.Background(), &pb.IDRequest{ID: id})
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

func main() {
	// Connect to the server (use "localhost:3000" for local testing)
	client, err := NewClient("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}()

	fmt.Println("=== Starting UserService RPC tests ===")
	fmt.Println("Current time:", time.Now().Format(time.RFC3339))

	// Test 1: Count
	count, err := client.Count()
	fmt.Printf("---Count users---\n")
	if err != nil {
		log.Printf("Count failed: %v", err)
	} else {
		fmt.Printf("Total users: %d\n", count)
	}

	// Test 2: GetAll
	fmt.Printf("---Get all users---\n")
	users, err := client.GetAll(50000)
	if err != nil {
		log.Printf("GetAll failed: %v", err)
	} else {
		totalUsers := len(users)
		fmt.Printf("Found %d users:\n", totalUsers)

		// Limit output to first 5 users for readability
		const maxDisplay = 20
		displayCount := min(maxDisplay, totalUsers)

		for i := range displayCount {
			u := users[i]
			fmt.Printf("  - ID: %d | Email: %s | Admin: %v\n", u.ID, u.Email, u.IsAdmin)
		}

		// Show remaining count if there are more users
		remaining := totalUsers - displayCount
		if remaining > 0 {
			fmt.Printf("  ... (more %d users)\n", remaining)
		}
	}

	// Test 3: Insert
	fmt.Printf("---Insert a new user---\n")
	newUser := &User{
		ID:        999,
		UUID:      "test-uuid-quang-2026",
		Email:     "quang.test@example.com",
		Password:  "securepass123",
		Image:     "https://example.com/avatar.jpg",
		CreatedAt: time.Now().Format(time.RFC3339),
		IsAdmin:   false,
	}
	success, err := client.Insert(newUser)
	if err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Printf("Insert user ID %d: %v\n", newUser.ID, success)
	}

	// Test 4: Get
	fmt.Printf("---Get the inserted user---\n")
	gotUser, err := client.Get(999)
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else {
		fmt.Printf("Retrieved user: ID = %d, Email = %s, CreatedAt = %s\n",
			gotUser.ID, gotUser.Email, gotUser.CreatedAt)
	}

	// Test 5: Set
	fmt.Printf("---Update (Set) a user---\n")
	newUser.Email = "quang.updated@example.com"
	newUser.IsAdmin = true
	success, err = client.Set(newUser)
	if err != nil {
		log.Printf("Set failed: %v", err)
	} else {
		fmt.Printf("Update user ID %d: %v\n", newUser.ID, success)
	}

	// Test 6: Delete
	fmt.Printf("---Delete a user---\n")
	success, err = client.Delete(999)
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		fmt.Printf("Delete user ID %d: %v\n", 999, success)
	}

	fmt.Println("=== All tests completed ===")
}

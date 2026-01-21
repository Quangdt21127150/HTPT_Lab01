package main

import (
	"context"
	"encoding/json"

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

type Client struct {
	conn   *grpc.ClientConn
	client pb.UserServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func (c *Client) GetUser(id int32) (*User, error) {
	resp, err := c.client.Get(context.Background(), &pb.GetRequest{Id: id})
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

func (c *Client) GetAllUsers() ([]*User, error) {
	resp, err := c.client.GetAll(context.Background(), &pb.GetAllRequest{})
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

func (c *Client) CountUsers() (int32, error) {
	resp, err := c.client.Count(context.Background(), &pb.CountRequest{})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (c *Client) InsertUser(user *User) error {
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	_, err = c.client.Insert(context.Background(), &pb.InsertRequest{UserJson: string(userJSON)})
	return err
}

func (c *Client) UpdateUser(user *User) error {
	userJSON, err := json.Marshal(user)
	if err != nil {
		return err
	}
	_, err = c.client.Set(context.Background(), &pb.SetRequest{UserJson: string(userJSON)})
	return err
}

func (c *Client) DeleteUser(id int32) error {
	_, err := c.client.Delete(context.Background(), &pb.DeleteRequest{Id: id})
	return err
}

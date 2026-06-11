package main

import (
	"encoding/json"
	"fmt"
	"log"
)

type User struct {
	Name string `json:"name"`
}

type UserCodec struct{}

func (jc *UserCodec) Encode(value interface{}) ([]byte, error) {
	user, ok := value.(User)
	if !ok {
		return nil, fmt.Errorf("unable to encode user: expected User, got %T", value)
	}

	return json.Marshal(user)
}

func (jc *UserCodec) Decode(data []byte) (interface{}, error) {
	var user User

	if err := json.Unmarshal(data, &user); err != nil {
		return nil, err
	}

	return user, nil
}

func main() {
	u := User{Name: "Test user"}
	log.Printf("before encode = %+v\n", u)

	jsonCodec := new(UserCodec)

	encoded, err := jsonCodec.Encode(u)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("encoded = %s\n", encoded)

	decodedUser, err := jsonCodec.Decode(encoded)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("after decode = %+v\n", decodedUser)
}

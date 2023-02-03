/*
 *
 * Copyright 2023 puzzleprofileserver authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package main

import (
	"log"
	"net"
	"os"

	mongoclient "github.com/dvaumoron/puzzlemongoclient"
	"github.com/dvaumoron/puzzleprofileserver/profileserver"
	pb "github.com/dvaumoron/puzzleprofileservice"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

func main() {
	if godotenv.Overload() == nil {
		log.Println("Loaded .env file")
	}

	lis, err := net.Listen("tcp", ":"+os.Getenv("SERVICE_PORT"))
	if err != nil {
		log.Fatal("Failed to listen :", err)
	}

	s := grpc.NewServer()
	pb.RegisterProfileServer(s, profileserver.New(mongoclient.Create()))
	log.Println("Listening at", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatal("Failed to serve :", err)
	}
}
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
package profileserver

import (
	"context"
	"errors"
	"log"

	pb "github.com/dvaumoron/puzzleprofileservice"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const collectionName = "profiles"

const idKey = "userId"
const descKey = "desc"
const infoKey = "info"
const pictureKey = "pictureData"

const mongoCallMsg = "Failed during MongoDB call :"

var errInternal = errors.New("internal service error")

var optsCreateUnexisting = options.Update().SetUpsert(true)
var optsExcludePictureField = options.Find().SetProjection(bson.D{{Key: pictureKey, Value: false}})
var optsOnlyPictureField = options.FindOne().SetProjection(bson.D{{Key: pictureKey, Value: true}})

// server is used to implement puzzleprofileservice.ProfileServer
type server struct {
	pb.UnimplementedProfileServer
	clientOptions *options.ClientOptions
	databaseName  string
}

func New(clientOptions *options.ClientOptions, databaseName string) pb.ProfileServer {
	return server{clientOptions: clientOptions, databaseName: databaseName}
}

func (s server) UpdateProfile(ctx context.Context, request *pb.UserProfile) (*pb.Response, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	id := request.UserId
	info := bson.M{}
	for k, v := range request.Info {
		info[k] = v
	}
	profile := bson.M{idKey: id, descKey: request.Desc, infoKey: info}
	collection := client.Database(s.databaseName).Collection(collectionName)
	_, err = collection.UpdateOne(
		ctx, bson.D{{Key: idKey, Value: id}}, profile, optsCreateUnexisting,
	)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func (s server) UpdatePicture(ctx context.Context, request *pb.Picture) (*pb.Response, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	id := request.UserId
	profile := bson.M{idKey: id, pictureKey: request.Data}
	collection := client.Database(s.databaseName).Collection(collectionName)
	_, err = collection.UpdateOne(
		ctx, bson.D{{Key: idKey, Value: id}}, profile, optsCreateUnexisting,
	)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func (s server) GetPicture(ctx context.Context, request *pb.UserId) (*pb.Picture, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)
	var result bson.D
	err = collection.FindOne(
		ctx, bson.D{{Key: idKey, Value: request.Id}}, optsOnlyPictureField,
	).Decode(&result)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}

	// can call [0] car result has only one field
	picture, _ := result[0].Value.(primitive.Binary)
	return &pb.Picture{UserId: request.Id, Data: picture.Data}, nil
}

func (s server) ListProfiles(ctx context.Context, request *pb.UserIds) (*pb.UserProfiles, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)
	cursor, err := collection.Find(ctx, idsFilter(request.Ids), optsExcludePictureField)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.UserProfiles{List: convertToProfiles(results)}, nil
}

func (s server) Delete(ctx context.Context, request *pb.UserId) (*pb.Response, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)
	_, err = collection.DeleteMany(ctx, bson.D{{Key: idKey, Value: request.Id}})
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func disconnect(client *mongo.Client, ctx context.Context) {
	if err := client.Disconnect(ctx); err != nil {
		log.Print("Error during MongoDB disconnect :", err)
	}
}

func idsFilter(ids []uint64) bson.D {
	filter := make(bson.A, 0, len(ids))
	for _, id := range ids {
		filter = append(filter, id)
	}
	return bson.D{{Key: idKey, Value: bson.D{{Key: "$in", Value: filter}}}}
}

func convertToProfiles(profiles []bson.M) []*pb.UserProfile {
	resProfiles := make([]*pb.UserProfile, 0, len(profiles))
	for _, profile := range profiles {
		desc, _ := profile[descKey].(string)
		info, _ := profile[infoKey].(bson.M)
		resInfo := map[string]string{}
		for k, v := range info {
			str, _ := v.(string)
			info[k] = str
		}
		resProfiles = append(resProfiles, &pb.UserProfile{
			UserId: extractUint64(profile[idKey]), Desc: desc, Info: resInfo,
		})
	}
	return resProfiles
}

func extractUint64(v any) uint64 {
	switch casted := v.(type) {
	case int32:
		return uint64(casted)
	case int64:
		return uint64(casted)
	}
	return 0
}

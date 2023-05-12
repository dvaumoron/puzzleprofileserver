#!/usr/bin/env bash

./build/build.sh

buildah from --name puzzleprofileserver-working-container scratch
buildah copy puzzleprofileserver-working-container $HOME/go/bin/puzzleprofileserver /bin/puzzleprofileserver
buildah config --env SERVICE_PORT=50051 puzzleprofileserver-working-container
buildah config --port 50051 puzzleprofileserver-working-container
buildah config --entrypoint '["/bin/puzzleprofileserver"]' puzzleprofileserver-working-container
buildah commit puzzleprofileserver-working-container puzzleprofileserver
buildah rm puzzleprofileserver-working-container

buildah push puzzleprofileserver docker-daemon:puzzleprofileserver:latest

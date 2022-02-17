#!/bin/bash

docker run --env-file env.list --name key-value-storage th2-key-value-storage:latest

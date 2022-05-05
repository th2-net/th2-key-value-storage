# Key-value storage

# Overview
This component is used to write data to a key-value store and retrieve data from that store. 

When data is written, an id is assigned to it, with the help of which access to this data will be possible.

It will connect to the cassandra database via the datastax driver and provide the ability to write and retrieve the data stored there using REST API.
# REST API

### GET REQUESTS

**`http://localhost:8080/getById` - returns an object from collection by id**

Accepts following query parameters:
- `collection` - text, name of collection from which object requested
- `id` - text, id of requested object
- `userId` - text, uuid-formatted string, identified user from whose part of storage data will be received
- `timestamp` - number, unix-format time when object was written (optional)


**`http://localhost:8080/getWorkspaceLink` - returns a workspace link by id**

Accepts following query parameters:
- `id` - text, id of requested link
- `userId` - text, uuid-formatted string, identified user from whose part of storage data will be received

**`http://localhost:8080/idsFromCollection` - returns a list of ids from collection**

Accepts following query parameters:
- `collection` - text, name of collection from which ids requested
- `userId` - text, uuid-formatted string, identified user from whose part of storage data will be received
- `sort` - text, get only "ascending" value. Means that ids will be sorted by timestamp in ascending order (default - descending) (optional)

**`http://localhost:8080/allRecordsFromCollection` - returns a list of records from collection**

Accepts following query parameters:
- `collection` - text, name of collection from which records requested
- `userId` - text, uuid-formatted string, identified user from whose part of storage data will be received
- `sort` - text, get only "ascending" value. Means that records will be sorted by timestamp in ascending order (default - descending) (optional)

**`http://localhost:8080/getUserPreferences` - returns a json-formatted user preferences**

Accepts following query parameters:
- `userId` - text, uuid-formatted string, identified user whose preferences will be received

### POST REQUESTS

**`http://localhost:8080/store` - stores data in the required collection and returns an id of stored data**

Request body fields:
- `id` - text, id-keyword for special data ('defaultUser', for example) (optional)
- `collection` - text, name of collection where the data will be stored
- `payload` - object, data to store
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be stored

**`http://localhost:8080/update` - updates record in collection by id**

Request body fields:
- `id` - text, id of record for update
- `collection` - text, name of collection where the data will be updated
- `payload` - object, updated data to store
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be updated

**`http://localhost:8080/updateUserPreferences` - updates user preferences**

Request body fields:
- `payload` - object, updated data to store
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be updated

**`http://localhost:8080/registerUser` - register user in storage, return user id**

Request body fields:
- `name` - text, user`s name
- `preferences` - object, user`s preferences data (optional)

### DELETE REQUESTS

**`http://localhost:8080/deleteRecordFromCollection` - delete record in collection by id**

Accepts following query parameters::
- `collection` - text, name of collection from where records will be deleted 
- `id` - text, id of record for delete
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be deleted

**`http://localhost:8080/dropCollection` - delete all records in collection**

Accepts following query parameters:
- `collection` - name of collection where records will be deleted
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be dropped

**`http://localhost:8080/clearCollection` - delete only not default records in collection**

Accepts following query parameters:
- `collection` - text, name of collection where not default records will be deleted
- `userId` - text, uuid-formatted string, identified user in whose part of storage data will be cleared
# How to copy data from one Elasticsearch instance to another
There are some ways to copy the data, here are a few

### Copy from Kibana Dev Tools
This is a post request from the Source elasticsearch instance to copy data using re-index to a destination Elasticsearch instance
```
# Copy data from Elastic to a remote Elastic.
POST /_reindex
{
    "source": {
        "remote": {
            "host": "${remoteHost}",
            "username": "${remoteUser}",
            "password": "${remotePwd}"
        },
        "index": "index_1"
    },
    "dest": {
        "index": "new_index"
    }
}
```
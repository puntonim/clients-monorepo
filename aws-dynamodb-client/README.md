**🟠 Clients monorepo: AWS DynamoDB Client**
=============================================

Just a Python client for AWS DynamoDB.\
It is a wrapper around (and an interface to) [boto3](https://boto3.amazonaws.com).


⚡ Usage
=======

See top docstrings in [dynamodb_client_table.py](aws_dynamodb_client/dynamodb_client_table.py).


Poetry install
--------------
From Github:
```sh
$ poetry add git+https://github.com/puntonim/clients-monorepo#subdirectory=aws-dynamodb-client
# at a specific version:
$ poetry add git+https://github.com/puntonim/clients-monorepo@00a49cb64524df19bf55ab5c7c1aaf4c09e92360#subdirectory=aws-dynamodb-client
```

From a local dir:
```sh
$ poetry add ../clients-monorepo/aws-dynamodb-client/
$ poetry add "aws-dynamodb-client @ file:///Users/myuser/workspace/clients-monorepo/aws-dynamodb-client/"
```

Pip install
-----------
Same syntax as Poetry, but change `poetry add` with `pip install`.


🛠️ Development setup
=====================

See [README.md](../README.md) in the root dir.


🚀 Deployment
=============

Libs are *not deployed* as they can be (pip-)installed directly from Github o local dir 
 (see Usage section in each lib's main README.md).\
And *not versioned* as when (pip-)installing from Github, it is possible to choose
 any version with a hash commit (see Usage section in each lib's main README.md).


©️ Copyright
=============

Copyright puntonim (https://github.com/puntonim). No License.

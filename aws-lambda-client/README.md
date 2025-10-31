**🟠 Clients monorepo: AWS Lambda Client**
==========================================

Just a Python client for AWS Lambda.\
It is a wrapper around (and an interface to) [boto3](https://boto3.amazonaws.com).


⚡ Usage
=======

See top docstrings in [aws_lambda_client.py](aws_lambda_client/aws_lambda_client.py).

Poetry install
--------------
From Github:
```sh
$ poetry add git+https://github.com/puntonim/clients-monorepo#subdirectory=aws-lambda-client
# at a specific version:
$ poetry add git+https://github.com/puntonim/clients-monorepo@00a49cb64524df19bf55ab5c7c1aaf4c09e92360#subdirectory=aws-lambda-client
```

From a local dir:
```sh
$ poetry add ../clients-monorepo/aws-lambda-client/
$ poetry add "aws-lambda-client @ file:///Users/myuser/workspace/clients-monorepo/aws-lambda-client/"
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

**🟠 Clients monorepo: AWS Parameter Store Client**
====================================================

Just a Python client for AWS Parameter Store.\
It is a wrapper around (and an interface to) [boto3](https://boto3.amazonaws.com).


⚡ Usage
=======

---

See top docstrings in [aws_parameter_store_client.py](aws_parameter_store_client/aws_parameter_store_client.py).


Poetry install
--------------
From Github:
```sh
$ poetry add git+https://github.com/puntonim/clients-monorepo#subdirectory=aws-parameter-store-client
# at a specific version:
$ poetry add git+https://github.com/puntonim/clients-monorepo@00a49cb64524df19bf55ab5c7c1aaf4c09e92360#subdirectory=aws-parameter-store-client
```

From a local dir:
```sh
$ poetry add ../clients-monorepo/aws-parameter-store-client/
$ poetry add "aws-parameter-store-client @ file:///Users/myuser/workspace/clients-monorepo/aws-parameter-store-client/"
```

Pip install
-----------
Same syntax as Poetry, but change `poetry add` with `pip install`.


🛠️ Development setup
=====================

---

See [README.md](../README.md) in the root dir.


©️ Copyright
=============

---

Copyright puntonim (https://github.com/puntonim). No License.

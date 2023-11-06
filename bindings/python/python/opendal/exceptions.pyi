# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class Error(Exception):
    """OpenDAL unrelated errors"""

    pass

class UnexpectedError(Exception):
    """Unexpected errors"""

    pass

class UnsupportedError(Exception):
    """Unsupported operation"""

    pass

class ConfigInvalidError(Exception):
    """Config is invalid"""

    pass

class NotFoundError(Exception):
    """Not found"""

    pass

class PermissionDeniedError(Exception):
    """Permission denied"""

    pass

class IsADirectoryError(Exception):
    """Is a directory"""

    pass

class NotADirectoryError(Exception):
    """Not a directory"""

    pass

class AlreadyExistsError(Exception):
    """Already exists"""

    pass

class IsSameFileError(Exception):
    """Is same file"""

    pass

class ConditionNotMatchError(Exception):
    """Condition not match"""

    pass

class ContentTruncatedError(Exception):
    """Content truncated"""

    pass

class ContentIncompleteError(Exception):
    """Content incomplete"""

    pass

class InvalidInputError(Exception):
    """Invalid input"""

    pass

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

class Unknown(Exception):
    """Unknown error"""

    pass

class Unexpected(Exception):
    """Unexpected errors"""

    pass

class Unsupported(Exception):
    """Unsupported operation"""

    pass

class ConfigInvalid(Exception):
    """Config is invalid"""

    pass

class NotFound(Exception):
    """Not found"""

    pass

class PermissionDenied(Exception):
    """Permission denied"""

    pass

class IsADirectory(Exception):
    """Is a directory"""

    pass

class NotADirectory(Exception):
    """Not a directory"""

    pass

class AlreadyExists(Exception):
    """Already exists"""

    pass

class IsSameFile(Exception):
    """Is same file"""

    pass

class ConditionNotMatch(Exception):
    """Condition not match"""

    pass

class ContentTruncated(Exception):
    """Content truncated"""

    pass

class ContentIncomplete(Exception):
    """Content incomplete"""

    pass

class InvalidInput(Exception):
    """Invalid input"""

    pass

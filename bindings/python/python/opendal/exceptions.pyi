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
    """Base class for exceptions in this module."""

class Unexpected(Error):
    """Unexpected errors"""

class Unsupported(Error):
    """Unsupported operation"""

class ConfigInvalid(Error):
    """Config is invalid"""

class NotFound(Error):
    """Not found"""

class PermissionDenied(Error):
    """Permission denied"""

class IsADirectory(Error):
    """Is a directory"""

class NotADirectory(Error):
    """Not a directory"""

class AlreadyExists(Error):
    """Already exists"""

class IsSameFile(Error):
    """Is same file"""

class ConditionNotMatch(Error):
    """Condition not match"""

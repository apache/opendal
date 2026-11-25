/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// <reference types="node" />

import { Writable, Readable } from 'node:stream'

function toWriterInput(chunk, encoding) {
  if (typeof chunk === 'string' && encoding && encoding !== 'utf8' && encoding !== 'utf-8') {
    return Buffer.from(chunk, encoding)
  }
  return chunk
}

class ReadStream extends Readable {
  constructor(reader, options) {
    super(options)
    this.reader = reader
  }

  async _read(size) {
    try {
      // Node can request size 0 when highWaterMark is 0.
      const chunk = await this.reader.readChunk(Math.min(Math.max(size, 1), 0xffffffff))
      if (!this.destroyed) {
        this.push(chunk)
      }
    } catch (error) {
      this.destroy(error)
    }
  }

  _destroy(error, callback) {
    this.reader = null
    callback(error)
  }
}

class BlockingReadStream extends Readable {
  constructor(reader, options) {
    super(options)
    this.reader = reader
  }

  _read(size) {
    try {
      // Node can request size 0 when highWaterMark is 0.
      const buf = Buffer.alloc(Math.max(size, 1))
      const s = this.reader.read(buf)
      if (s === 0n) {
        this.push(null)
      } else {
        this.push(buf.subarray(0, Number(s)))
      }
    } catch (error) {
      this.destroy(error)
    }
  }

  _destroy(error, callback) {
    this.reader = null
    callback(error)
  }
}

class WriteStream extends Writable {
  constructor(writer, options) {
    super(options)
    this.writer = writer
  }

  _write(chunk, encoding, callback) {
    let data
    try {
      data = toWriterInput(chunk, encoding)
    } catch (error) {
      callback(error)
      return
    }

    this.writer
      .write(data)
      .then(() => {
        callback()
      })
      .catch((error) => {
        callback(error)
      })
  }

  _final(callback) {
    this.writer
      .close()
      .then(() => {
        callback()
      })
      .catch((error) => {
        callback(error)
      })
  }
}

class BlockingWriteStream extends Writable {
  constructor(writer, options) {
    super(options)
    this.writer = writer
  }

  _write(chunk, encoding, callback) {
    try {
      this.writer.write(toWriterInput(chunk, encoding))
      callback()
    } catch (error) {
      callback(error)
    }
  }

  _final(callback) {
    try {
      this.writer.close()
      callback()
    } catch (error) {
      callback(error)
    }
  }
}

import * as generated from './generated.js'
const {
  CapabilityOverrideLayer,
  ConcurrentLimitLayer,
  LoggingLayer,
  RetryLayer,
  ThrottleLayer,
  TimeoutLayer,
  BlockingReader,
  Reader,
  BlockingWriter,
  Writer,
} = generated

BlockingReader.prototype.createReadStream = function (options) {
  return new BlockingReadStream(this, options)
}

Reader.prototype.createReadStream = function (options) {
  return new ReadStream(this, options)
}

BlockingWriter.prototype.createWriteStream = function (options) {
  return new BlockingWriteStream(this, options)
}

Writer.prototype.createWriteStream = function (options) {
  return new WriteStream(this, options)
}

export const layers = {
  CapabilityOverrideLayer,
  ConcurrentLimitLayer,
  LoggingLayer,
  RetryLayer,
  ThrottleLayer,
  TimeoutLayer,
}

export * from './generated.js'
export default generated

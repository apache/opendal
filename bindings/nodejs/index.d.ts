/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export const enum ObjectMode {
  /** FILE means the object has data to read. */
  FILE = 0,
  /** DIR means the object can be listed. */
  DIR = 1,
  /** Unknown means we don't know what we can do on this object. */
  Unknown = 2
}
export class Memory {
  constructor()
  object(path: string): DataObject
}
export class Fs {
  constructor(options: Record<string, string>)
  object(path: string): DataObject
}
export class Operator {
  constructor(serviceType: string, options?: Record<string, string> | undefined | null)
  object(path: string): DataObject
}
export class ObjectMetadata {
  /** Mode of this object. */
  get mode(): ObjectMode
  /** Content-Disposition of this object */
  get contentDisposition(): string | null
  /** Content Length of this object */
  get contentLength(): bigint | null
  /** Content MD5 of this object. */
  get contentMd5(): string | null
  /** Content Type of this object. */
  get contentType(): string | null
  /** ETag of this object. */
  get etag(): string | null
  /** Last Modified of this object.(UTC) */
  get lastModified(): string | null
}
export class ObjectLister {
  next(): Promise<DataObject | null>
}
export class DataObject {
  stat(): Promise<ObjectMetadata>
  statSync(): ObjectMetadata
  write(content: Buffer): Promise<void>
  writeSync(content: Buffer): void
  read(): Promise<Buffer>
  readSync(): Buffer
  scan(): Promise<ObjectLister>
  delete(): Promise<void>
  deleteSync(): void
}

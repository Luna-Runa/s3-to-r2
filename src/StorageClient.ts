import {
  CORSRule,
  GetObjectCommand,
  GetObjectCommandOutput,
  ListObjectsCommand,
  PutBucketCorsCommand,
  S3Client,
  S3ClientConfig,
} from '@aws-sdk/client-s3'

interface StorageClientConfig extends S3ClientConfig {}

export class StorageClient {
  private client: S3Client
  bucket: string

  constructor(bucket: string, config: StorageClientConfig) {
    this.client = new S3Client(config)
    this.bucket = bucket
  }

  async getKeysByBucket(): Promise<string[]> {
    try {
      const files = await this.client.send(new ListObjectsCommand({ Bucket: this.bucket }))

      const keys = files.Contents ? files.Contents.map(item => item.Key) : []
      console.log(`get ${keys.length} keys by ${this.bucket}`)

      return keys
    } catch (err) {
      console.log(`getKeysByBucket Error : ${this.bucket}`, err)
    }
  }

  async getObjectByKey(key: string): Promise<GetObjectCommandOutput> {
    try {
      const data = await this.client.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }))

      return data
    } catch (err) {
      console.log(`getFileByKey Error : ${key} \n`, err)
    }
  }

  async putBucketCors(corsRule: CORSRule): Promise<void> {
    try {
      const data = await this.client.send(
        new PutBucketCorsCommand({ Bucket: this.bucket, CORSConfiguration: { CORSRules: [corsRule] } }),
      )
    } catch (err) {
      console.log(`putBucketCors Error : ${this.bucket}`)
    }
  }
}

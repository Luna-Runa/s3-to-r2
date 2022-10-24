import S3, { PutObjectRequest } from 'aws-sdk/clients/s3.js'
import fs from 'fs'
import { StorageClient } from './StorageClient.js'
import { CORSRule } from '@aws-sdk/client-s3'
import dotenv from 'dotenv'

dotenv.config()

const sourceStorageClient: StorageClient = new StorageClient(process.env.SOURCE_BUCKET, {
  region: process.env.SOURCE_STORAGE_REGION,
  endpoint: process.env.SOURCE_STORAGE_ENDPOINT,
  credentials: {
    accessKeyId: process.env.SOURCE_STORAGE_ACCESS_KEY_ID,
    secretAccessKey: process.env.SOURCE_STORAGE_SECRET_ACCESS_KEY,
  },
})

const destinationStorageClient: StorageClient = new StorageClient(process.env.DESTINATION_BUCKET, {
  region: process.env.DESTINATION_STORAGE_REGION,
  endpoint: process.env.DESTINATION_STORAGE_ENDPOINT,
  credentials: {
    accessKeyId: process.env.DESTINATION_STORAGE_ACCESS_KEY_ID,
    secretAccessKey: process.env.DESTINATION_STORAGE_SECRET_ACCESS_KEY,
  },
})

// R2에 업로드할때 진행도를 알기 위해 aws-sdk v2 사용
const destinationStorageClientAwsSdkV2 = new S3({
  endpoint: process.env.DESTINATION_STORAGE_ENDPOINT,
  accessKeyId: process.env.DESTINATION_STORAGE_ACCESS_KEY_ID,
  secretAccessKey: process.env.DESTINATION_STORAGE_SECRET_ACCESS_KEY,
  signatureVersion: 'v4',
})

/// if destinationStorage is R2, run this once --------------------------
const corsRule: CORSRule = {
  AllowedHeaders: ['*'],
  AllowedMethods: ['HEAD', 'GET', 'PUT', 'POST', 'DELETE'],
  AllowedOrigins: ['*'],
  ExposeHeaders: ['ETag'],
}

destinationStorageClient.putBucketCors(corsRule)
// ---------------------------------------------------------------------

const sourceKeys = await sourceStorageClient.getKeysByBucket()
const destinationKeys = await destinationStorageClient.getKeysByBucket()

const unUploadedKeys = sourceKeys.filter(key => !destinationKeys.includes(key))

const keyPage: string[][] = []
// 최대 pagePer 만큼만 전송을 병렬로 실행하기 위해 2차원 배열로 나눔
// 업로드중 너무 높을시 RequestTimeTooSkewed 에러가 생길 수 있음
const pagePer = 8
for (let i = 0; i < unUploadedKeys.length / pagePer; i++) {
  const currentPage = i * pagePer
  keyPage.push(unUploadedKeys.slice(currentPage, currentPage + pagePer))
}

let keyPageCount = 1
let uploadedCount = 1

for (const keys of keyPage) {
  uploadedCount = 1

  logging('Page', keyPageCount++, keyPage.length)

  await Promise.all(
    keys.map(async key => {
      try {
        const { ContentLength, ContentDisposition, Body, ContentType } = await sourceStorageClient.getObjectByKey(key)

        const fileName = ContentDisposition ? ContentDisposition.split('filename=')[1] : key

        await uploadFile(
          destinationStorageClientAwsSdkV2,
          {
            Bucket: destinationStorageClient.bucket,
            Key: key,
            ContentLength,
            ContentDisposition,
            Body,
            ContentType,
          },
          fileName,
        )

        logging('File', uploadedCount++, keys.length, fileName)

        return uploadFile
      } catch (err) {
        console.log('uploadFile Error : ', err)
      }
    }),
  )
}

function logging(type: 'Page' | 'File', currentCount: number, totalCount: number, fileName?: string) {
  if (type === 'Page') {
    console.log(`Page ${currentCount} / ${totalCount} Start ${'-'.repeat(32)}`)
    fs.appendFileSync('log.txt', `Page ${currentCount} / ${totalCount} Start ${'-'.repeat(32)}\n`)
  } else {
    console.log(`File ${currentCount} / ${totalCount} Complete!`)
    fs.appendFileSync('log.txt', `File ${currentCount} / ${totalCount} Complete!    ${fileName}\n`)
  }
}

async function uploadFile(
  destinationStorageClientAwsSdkV2: S3,
  uploadBucketParams: PutObjectRequest,
  fileName: string,
) {
  const upload = destinationStorageClientAwsSdkV2.upload(uploadBucketParams, (err, data) => {})

  upload.on('httpUploadProgress', progress => {
    // progress.total is not working
    console.log(
      `  - ${fileName.padEnd(50, ' ')} progress : ${(progress.loaded / uploadBucketParams.ContentLength) * 100}`,
    )
  })

  return upload.promise()
}

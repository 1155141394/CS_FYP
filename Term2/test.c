#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <iostream>

int main()
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    {
        Aws::S3::S3Client s3_client;

        auto list_buckets_outcome = s3_client.ListBuckets();
        if (!list_buckets_outcome.IsSuccess())
        {
            std::cout << "Failed to list buckets:" << list_buckets_outcome.GetError().GetMessage()
                      << std::endl;
            return 1;
        }

        std::cout << "Buckets:" << std::endl;
        for (const auto& bucket : list_buckets_outcome.GetResult().GetBuckets())
        {
            std::cout << "  " << bucket.GetName() << std::endl;

            Aws::S3::Model::ListObjectsRequest objects_request;
            objects_request.SetBucket(bucket.GetName());

            auto list_objects_outcome = s3_client.ListObjects(objects_request);
            if (list_objects_outcome.IsSuccess())
            {
                std::cout << "    Objects:" << std::endl;
                for (const auto& object : list_objects_outcome.GetResult().GetContents())
                {
                    std::cout << "      " << object.GetKey() << std::endl;
                }
            }
            else
            {
                std::cout << "    Failed to list objects:" << list_objects_outcome.GetError().GetMessage()
                          << std::endl;
            }
        }
    }

    Aws::ShutdownAPI(options);

    return 0;
}

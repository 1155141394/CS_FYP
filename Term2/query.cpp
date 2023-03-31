#include <stdio.h>
#include <stdlib.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/SelectObjectContentRequest.h>

#include <aws/s3/model/CSVInput.h>
#include <aws/s3/model/CSVOutput.h>
#include <aws/s3/model/RecordsEvent.h>
#include <aws/s3/model/StatsEvent.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <cmath>

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace std;

vector<int> compress_array(vector<vector<int>> arr) {
    int rows = arr.size();
    int cols = arr[0].size();
    vector<int> compressed_arr;
    int count = 0;
    for (int i = 0; i < rows; ++i) {
        for (int j = 0; j < cols; ++j) {
            if (arr[i][j] == 0) {
                count++;
            } else {
                if (count > 0) {
                    compressed_arr.push_back(-count);
                    count = 0;
                }
                compressed_arr.push_back(arr[i][j]);
            }
        }
    }
    if (count > 0) {
        compressed_arr.push_back(-count);
    }
    compressed_arr.push_back(rows);
    compressed_arr.push_back(cols);
    return compressed_arr;
}

vector<vector<int>> decompress_array(vector<int> compressed_arr) {
    int rows = compressed_arr[compressed_arr.size() - 2];
    int cols = compressed_arr[compressed_arr.size() - 1];
    vector<vector<int>> decompressed_arr(rows, vector<int>(cols, 0));
    int i = 0, j = 0, k = 0;
    while (k < compressed_arr.size() - 2) {
        if (compressed_arr[k] < 0) {
            j += abs(compressed_arr[k]);
        } else {
            decompressed_arr[i][j] = compressed_arr[k];
            j++;
        }
        if (j >= cols) {
            i++;
            j -= cols;
        }
        k++;
    }
    return decompressed_arr;
}


vector<int> find_rows(std::vector<std::vector<int>> arr, int index1, int index2) {
    std::vector<int> rows;
    for (int i = 0; i < arr.size(); i++) {
        std::vector<int> row = arr[i];
        if (index1 != -1 && index2 != -1) {
            if (row[index1] == 1 && row[index2] == 1) {
                rows.push_back(i);
            }
        } else if (index1 == -1 && index2 != -1) {
            if (row[index2] == 1) {
                rows.push_back(i);
            }
        } else if (index1 != -1 && index2 == -1) {
            if (row[index1] == 1) {
                rows.push_back(i);
            }
        }
    }
    return rows;
}






//std::vector<std::string> s3_select(std::string bucket_name, std::string object_key, std::string expression)
//{
//    Aws::SDKOptions options;
//    Aws::InitAPI(options);
//
//    std::vector<std::string> rows;
//
//    // Create an S3Client object
//    Aws::Client::ClientConfiguration client_config;
//    client_config.region = "us-west-2"; // change the region as necessary
//    S3Client s3_client(client_config);
//
//    // Set up the SelectObjectContentRequest
//    SelectObjectContentRequest request;
//    request.SetBucket(bucket_name);
//    request.SetKey(object_key);
//    request.SetExpression(expression);
//
//    // Set up the input serialization
//    Aws::S3::Model::InputSerialization input_serialization;
//    request.SetInputSerialization(input_serialization);
//
//    // Set up the output serialization
//    Aws::S3::Model::OutputSerialization csv_output;
//    csv_output.SetCSV(Aws::S3::Model::CSVOutput());
////    csv_output.GetCSV().SetRecordDelimiter("\n");
////    csv_output.GetCSV().SetFieldDelimiter(",");
//    request.SetOutputSerialization(csv_output);
//
//    // Execute the request and retrieve the results
//    auto outcome = s3_client.SelectObjectContent(request);
//    if (!outcome.IsSuccess())
//    {
//        std::cout << "Failed to retrieve data from S3: " << outcome.GetError().GetMessage() << std::endl;
//        return rows;
//    }
//    else{
//        bool isRecordsEventReceived = false;
//        bool isStatsEventReceived = false;
//        SelectObjectContentHandler handler;
//        handler.SetRecordsEventCallback([&](const RecordsEvent& recordsEvent)
//        {
//            isRecordsEventReceived = true;
//            auto recordsVector = recordsEvent.GetPayload();
//            Aws::String records(recordsVector.begin(), recordsVector.end());
//
//        });
//        request.SetEventStreamHandler(handler);
//
//        auto selectObjectContentOutcome = Client->SelectObjectContent(request);
//        ASSERT_TRUE(selectObjectContentOutcome.IsSuccess());
//        ASSERT_TRUE(isRecordsEventReceived);
//        ASSERT_TRUE(isStatsEventReceived);
//    }
//
//
//
//    Aws::ShutdownAPI(options);
//
//    return rows;
//}

std::vector<int> time_index(const std::tm* start_t, const std::tm* end_t) {
    std::vector<int> hours;

    if (start_t == nullptr) {
        int end_h = end_t->tm_hour;
        int end_index = end_h / 2 + 1;
        for (int i = 1; i <= end_index; i++) {
            hours.push_back(i);
        }
    } else if (end_t == nullptr) {
        int start_h = start_t->tm_hour;
        int start_index = start_h / 2 + 1;
        for (int i = start_index; i <= 12; i++) {
            hours.push_back(i);
        }
    } else {
        int start_h = start_t->tm_hour;
        int end_h = end_t->tm_hour;
        int start_index = start_h / 2 + 1;
        int end_index = end_h / 2 + 1;
        for (int i = start_index; i <= end_index; i++) {
            hours.push_back(i);
        }
    }

    return hours;
}

int main()
{
//    std::string bucket_name = "my-bucket";
//    std::string object_key = "path/to/my-object";
//    std::string expression = "SELECT * FROM S3Object";
//
//    std::vector<std::string> rows = s3_select(bucket_name, object_key, expression);
//
//    // Process the rows as necessary
//    for (const auto& row : rows)
//    {
//        std::cout << row << std::endl;
//    }
    std::vector<std::vector<int>> vec = { { 0,1,0 },
                                      { 1,0,0 } };
    vector<int> path = compress_array(vec);
    for (int i = 0; i < path.size(); i++) {
        std::cout << pth[i] << ' ';
    }


    return 0;
}




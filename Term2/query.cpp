#include <stdio.h>
#include <stdlib.h>
//#include <aws/core/Aws.h>
//#include <aws/s3/S3Client.h>
//#include <aws/s3/model/SelectObjectContentRequest.h>
//
//#include <aws/s3/model/CSVInput.h>
//#include <aws/s3/model/CSVOutput.h>
//#include <aws/s3/model/RecordsEvent.h>
//#include <aws/s3/model/StatsEvent.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <ctime>

//using namespace Aws;
//using namespace Aws::S3;
//using namespace Aws::S3::Model;
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

tm StringToDatetime(std::string str)
{
    char *cha = (char*)str.data();             // 将string转换成char*。
    tm tm_;                                    // 定义tm结构体。
    int year, month, day, hour, minute, second;// 定义时间的各个int临时变量。
    sscanf(cha, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second);// 将string存储的日期时间，转换为int临时变量。
    tm_.tm_year = year - 1900;                 // 年，由于tm结构体存储的是从1900年开始的时间，所以tm_year为int临时变量减去1900。
    tm_.tm_mon = month - 1;                    // 月，由于tm结构体的月份存储范围为0-11，所以tm_mon为int临时变量减去1。
    tm_.tm_mday = day;                         // 日。
    tm_.tm_hour = hour;                        // 时。
    tm_.tm_min = minute;                       // 分。
    tm_.tm_sec = second;                       // 秒。
    tm_.tm_isdst = 0;                          // 非夏令时。
    return tm_;                                 // 返回值。
}

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
// range-based loop
void PrintVecofVec1(vector<vector<int>>& res) {
  for (auto& rowV : res) {
    for (auto& el : rowV) {
      cout << el << " ";
    }
    cout << "; ";
  }
  cout << endl;
}


int main()
{
    char* test = "2023-01-01 12:00:12";
    tm tm_;
    strptime(test, "%Y-%m-%d %H:%M:%S", &tm_);
    cout << tm_.tm_year << endl;
    cout << tm_.tm_mon << endl;
    cout << tm_.tm_mday << endl;
    cout << tm_.tm_sec << endl;



    return 0;
}




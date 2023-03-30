#include <stdio.h>
#include <stdlib.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/SelectObjectContentRequest.h>
#include <aws/s3/model/InputSerialization.h>
#include <aws/s3/model/OutputSerialization.h>
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



void s3_select() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    Aws::S3::S3Client s3_client;

    // Set up the request
    Aws::S3::Model::SelectObjectContentRequest request;
    request.SetBucket("my-bucket");
    request.SetKey("my-file.csv");
    request.SetExpression("SELECT * FROM S3Object");
    request.SetExpressionType(Aws::S3::Model::ExpressionType::SQL);

    // Set up the input serialization
    Aws::S3::Model::InputSerialization input_serialization;
    input_serialization.SetCSV(Aws::S3::Model::CSVInput());
    input_serialization.GetCSV().SetFileHeaderInfo(Aws::S3::Model::FileHeaderInfo::USE);
    input_serialization.GetCSV().SetRecordDelimiter("\n");
    input_serialization.GetCSV().SetFieldDelimiter(",");
    request.SetInputSerialization(input_serialization);

    // Set up the output serialization
    Aws::S3::Model::OutputSerialization output_serialization;
    output_serialization.SetCSV(Aws::S3::Model::CSVOutput());
    output_serialization.GetCSV().SetRecordDelimiter("\n");
    output_serialization.GetCSV().SetFieldDelimiter(",");
    request.SetOutputSerialization(output_serialization);

    // Make the request
    auto outcome = s3_client.SelectObjectContent(request);

    if (outcome.IsSuccess()) {
        // Read the result and store it in a string
        std::string result;
        auto &response_payload = outcome.GetResult().GetPayload();
        Aws::IOStreamToString(result, &response_payload);
        std::cout << result << std::endl;
    } else {
        std::cout << "Error: " << outcome.GetError().GetMessage() << std::endl;
    }

    Aws::ShutdownAPI(options);
}
int main(){
    s3_select();
}



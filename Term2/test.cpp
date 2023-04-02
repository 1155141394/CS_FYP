#include <stdio.h>
#include <stdlib.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/SelectObjectContentRequest.h>

#include <aws/s3/model/CSVInput.h>
#include <aws/s3/model/CSVOutput.h>
#include <aws/s3/model/RecordsEvent.h>
#include <aws/s3/model/StatsEvent.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <ctime>
#include <string>

using namespace Aws;
using namespace Aws::S3;
using namespace Aws::S3::Model;


Aws::string s3_select(std::string bucket_name, std::string object_key, std::string expression)
{
    Aws::SDKOptions options;
//        request.SetResponseStreamFactory([] { return new std::fstream("jianming.csv", std::ios_base::out); });
    Aws::InitAPI(options);
    Aws::String s3_result;
//    std::vector<std::string> rows;
    // Create an S3Client object
    Aws::Client::ClientConfiguration client_config;
//    client_config.endpointOverride = "127.0.0.1/"
    client_config.scheme = Aws::Http::Scheme::HTTP;
    client_config.verifySSL = false;
    client_config.region = "ap-northeast-1"; // change the region as necessary
    S3Client s3_client(client_config);
//    std::shared_ptr<S3Client> client;
    //cout << "Create client" << endl;

    // Set up the SelectObjectContentRequest
    SelectObjectContentRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);
    request.SetExpressionType(S3::Model::ExpressionType::SQL);
    request.SetExpression(expression);

    // Set up the input serialization
    CSVInput csvInput;
    csvInput.SetFileHeaderInfo(FileHeaderInfo::NONE);
    InputSerialization inputSerialization;
    inputSerialization.SetCSV(csvInput);
    request.SetInputSerialization(inputSerialization);

    // Set up the output serialization
    CSVOutput csvOutput;
    OutputSerialization outputSerialization;
    outputSerialization.SetCSV(csvOutput);
    request.SetOutputSerialization(outputSerialization);

    // Execute the request and retrieve the results
    bool isRecordsEventReceived = false;
    bool isStatsEventReceived = false;

    //cout << "Query setting finished" << endl;
    SelectObjectContentHandler handler;
    //cout << "Set handler" << endl;
    handler.SetRecordsEventCallback([&](const RecordsEvent& recordsEvent)
    {
        //cout << "Set records event callback" << endl;
        isRecordsEventReceived = true;
        auto recordsVector = recordsEvent.GetPayload();
        //cout << "Get payload." << endl;
        Aws::String records(recordsVector.begin(), recordsVector.end());
        //cout << "Get records" << endl;
//        cout << "Get string successfully." << endl;
//        return records.c_str();
//        std::string s(records.c_str(), records.size());
        s3_result = records;
        //ASSERT_STREQ(firstColumn.c_str(), records.c_str());
    });
    //cout << "SetRecordsEventCallback" << endl;
    handler.SetStatsEventCallback([&](const StatsEvent& statsEvent)
    {
        isStatsEventReceived = true;
//        ASSERT_EQ(static_cast<long long>(objectSize), statsEvent.GetDetails().GetBytesScanned());
//        ASSERT_EQ(static_cast<long long>(objectSize), statsEvent.GetDetails().GetBytesProcessed());
//        ASSERT_EQ(static_cast<long long>(firstColumn.size()), statsEvent.GetDetails().GetBytesReturned());
    });
    //cout << "SetStatsEventCallback" << endl;

    request.SetEventStreamHandler(handler);

    auto selectObjectContentOutcome = s3_client.SelectObjectContent(request);

    if (!selectObjectContentOutcome.IsSuccess()) {
        const Aws::S3::S3Error &err = selectObjectContentOutcome.GetError();
        std::cerr << "Error: GetObject: " <<
                  err.GetExceptionName() << ": " << err.GetMessage() << std::endl;
    }
    else {
        std::cout << "Successfully retrieved!" << std::endl;
    }

//    std::string s(s3_result.c_str(), s3_result.size());
//    std::cout << s << std::endl;


    Aws::ShutdownAPI(options);

    return s3_result;
}


int main(){
    Aws::string s3_result;
    s3_result = s3_select("fypts", "0/2023-01-01_12.csv", "SELECT * FROM s3object limit 5");
    std::cout << s3_result << std::endl;
}
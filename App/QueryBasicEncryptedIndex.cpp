#include <chrono>
  
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "App.h"
#include "Simple9.h"
#include "Enclave_u.h"

void query_basic_encrypted_index(const char* encrypted_index_file_name,
                                 const char* posting_info_file_name,
                                 const char* query_file_name,
                                 const char* dataset_name) {
  std::string drop_cache_data = "1";
  std::map<std::string, std::pair<int64_t, int64_t>> postings_info;
  std::map<std::string, int64_t> term_df_info;

  std::string line;
  std::ifstream postings_info_file(posting_info_file_name, std::ios::in);

  if (postings_info_file.is_open()) {
    while (getline(postings_info_file, line))
    {
      std::istringstream iss(line);
      std::vector<std::string> line_split(
          std::istream_iterator<std::string>{iss},
          std::istream_iterator<std::string>());

      postings_info.insert(std::make_pair(line_split[0],
                                          std::make_pair(
                                            std::atoll(line_split[2].c_str()),
                                            std::atoll(line_split[3].c_str())
                                          ))
                           );
      term_df_info.insert(std::make_pair(line_split[0],
                                         std::atoll(line_split[1].c_str())));
    }
    postings_info_file.close();
  }
  else
  {
    std::cout << "Unable to open postings info file." << std::endl;
  }

  std::ifstream query_file(query_file_name, std::ios::in);
  if (query_file.is_open()) {
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;
    while (getline(query_file, line)) {
      bool succ_query = true;
      std::istringstream iss(line);
      std::vector<std::string> query_keywords(
          std::istream_iterator<std::string>{iss},
          std::istream_iterator<std::string>());

      // offset, length, document_frequency.
      int64_t *keyword_info_struct = (int64_t *) malloc(3 * query_keywords.size() * sizeof(int64_t));
      for (int i = 0; i < query_keywords.size(); i++) {
        if (postings_info.find(query_keywords[i]) != postings_info.end()) {
          keyword_info_struct[i * 3 + 0] = postings_info[query_keywords[i]].first;
          keyword_info_struct[i * 3 + 1] = postings_info[query_keywords[i]].second;
          keyword_info_struct[i * 3 + 2] = term_df_info[query_keywords[i]];
        } else {
          succ_query = false;
        }
      }

      
      if (succ_query) {
        // Drop caches. Need Sudo.
        sync();
        int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
        int unused = write(fd, drop_cache_data.c_str(), sizeof(char));
        close(fd);

        ret = ecall_query_basic_encrypted_index(
            global_eid,
            (uint8_t *)keyword_info_struct,
            query_keywords.size() * 3 * sizeof(int64_t) / sizeof(uint8_t),
            encrypted_index_file_name,
            dataset_name,
            "wave");
        if (ret != SGX_SUCCESS) {
          std::cout << "Error: ecall_query_basic_encrypted_index." << std::endl;
          break;
        }
      } else {
        // std::cout << "Error: Cannot find query keyword info: " << line << std::endl;
        continue;
      }
      
      free(keyword_info_struct);
    }
    query_file.close();
  } else {
    std::cout << "Unable to open query file." << std::endl;
  }
}

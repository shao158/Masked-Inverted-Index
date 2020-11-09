#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "App.h"
#include "Enclave_u.h"

void generate_basic_encrypted_index(const char* plain_posting_file,
                                    const char* encrypted_index_file) {
  std::string line;
  std::ifstream postings_file(plain_posting_file, std::ios::in);
  int line_count = 0;

  if (postings_file.is_open()) {
    std::string current_term;
    int64_t document_frequency = 0;

    sgx_status_t ret = SGX_ERROR_UNEXPECTED;
    SGX_FILE *index_fp;
    ret = ecall_file_open(global_eid, &index_fp, encrypted_index_file, "w");
    if (ret != SGX_SUCCESS) {
      std::cout << "Error: ecall_file_open - " << encrypted_index_file << std::endl;
      abort();
    }

    int32_t *data_buffer = new int32_t[2 * 1000 * 1000];
    uint8_t *encoded_data = new uint8_t[4 * 2 * 1000 * 1000];

    while (++line_count && getline(postings_file, line)) {
      std::istringstream iss(line);
      std::vector<std::string> line_split(
          std::istream_iterator<std::string>{iss},
          std::istream_iterator<std::string>());

      if (line_count % 2 == 1) { // Line format: term term_id document_frequency
        current_term = line_split[0];
        document_frequency = std::stoll(line_split[2]);
      } else {
        // Ignore postings with only 1 document.
        if (line_split.size() < 3) continue;

        uint64_t before_file_size = 0;
        ret = ecall_file_get_file_size(global_eid, &before_file_size, index_fp);
        if (ret != SGX_SUCCESS) {
          std::cout << "Error: ecall_file_get_size." << std::endl;
          abort();
        }

        std::cout << current_term
                  << " " << document_frequency
                  << " " << before_file_size;

        for (size_t i = 0; i < line_split.size(); i += 1)
        {
          if (i % 2 == 0 && i > 0) { // Doc id. Delta encoding.
            data_buffer[i] = (std::stoi(line_split[i]) - std::stoi(line_split[i - 2]));
          } else { // Term frequency.
            data_buffer[i] = std::stoi(line_split[i]);
          }
        }

        size_t encoded_len = simple9_encode((uint32_t *)data_buffer,
                                            line_split.size(), encoded_data);

        size_t write_size = 0;
        ret = ecall_file_write(global_eid, &write_size, index_fp, encoded_data, encoded_len);
        if (ret != SGX_SUCCESS || write_size != encoded_len) {
          std::cout << "Error: ecall_file_write." << std::endl;
          abort();
        }

        uint64_t after_file_size = 0;
        ret = ecall_file_get_file_size(global_eid, &after_file_size, index_fp);
        if (ret != SGX_SUCCESS) {
          std::cout << "Error: ecall_file_get_size." << std::endl;
          abort();
        }

        if (after_file_size - before_file_size != write_size) {
          std::cout << "Error: write size does not match." << std::endl;
          abort();
        }

        std::cout << " " << write_size << std::endl;
      }
    }

    delete encoded_data;
    delete data_buffer;
    postings_file.close();

    int32_t fileHandle;
    ret = ecall_file_close(global_eid, &fileHandle, index_fp);
    if (ret != SGX_SUCCESS) {
      std::cout << "Error: ecall_file_close." << std::endl;
      abort();
    }
  }
  else
  {
    std::cout << "Unable to open postings file." << std::endl;
  }
}

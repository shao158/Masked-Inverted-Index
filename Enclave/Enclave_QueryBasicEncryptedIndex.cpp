#include "Enclave.h"
#include "Enclave_t.h"
#include "Simple9.h"

#include <algorithm>
#include <cmath>
#include <vector>


bool sortByScore(const std::pair<int32_t, float> &a,
                 const std::pair<int32_t, float> &b)
{
  return (a.second > b.second);
}

void ecall_query_basic_encrypted_index(uint8_t* keyword_data, uint64_t keyword_data_size,
                                       const char* encrypted_index_file_name,
                                       const char* dataset_name, const char* merge_method) {
  // trec45 / clueweb.
  const double NUM_TOTAL_DOCUMENTS = !strcmp(dataset_name, "trec45") ? 528155 : 33836981; 

  int64_t *cast_keyword_data = (int64_t *)keyword_data;
  const int num_keywords = keyword_data_size * sizeof(uint8_t) / sizeof(int64_t) / 3;
  std::vector<int64_t> keyword_offset;
  std::vector<int64_t> keyword_length;
  std::vector<int64_t> keyword_doc_frequency;
  for (int i = 0; i < num_keywords; i++) {
    keyword_offset.push_back(cast_keyword_data[i * 3]);
    keyword_length.push_back(cast_keyword_data[i * 3 + 1]);
    keyword_doc_frequency.push_back(cast_keyword_data[i * 3 + 2]);
  }

  SGX_FILE *index_fd;
  index_fd = sgx_fopen_auto_key(encrypted_index_file_name, "r");
  if (!index_fd) {
    printf_fmt("Error in opening file.\n");
    return;
  }

  // Extract postings.
  int64_t total_data_fetched = 0;
  std::vector<int32_t> keyword_posting_size(num_keywords, 0);
  uint32_t **postings = (uint32_t **) malloc(num_keywords * sizeof(uint32_t *));
  for (int i = 0; i < num_keywords; i++) {
    if (sgx_fseek(index_fd, keyword_offset[i], SEEK_SET)) {
      printf_fmt("Error in fseek.\n");
      return;
    }

    uint8_t *encoded_data;
    encoded_data = (uint8_t *)malloc(keyword_length[i] * sizeof(uint8_t));
    size_t size_read = sgx_fread(encoded_data, sizeof(uint8_t), keyword_length[i], index_fd);

    if (size_read != sizeof(uint8_t) * keyword_length[i]) {
      printf_fmt("Error in fread.\n");
      return;
    }
    
    total_data_fetched += size_read;

    // This decoding process can be non-oblivious.

    size_t size_data;
    postings[i] = NULL;
    simple9_decode(postings + i, &size_data, encoded_data);

    // Each doc has two uint32_t, doc_id and term_frequency.
    keyword_posting_size[i] = size_data / 2;
 
    // Restore doc_id from delta encoding.
    for (size_t j = 2; j < size_data; j += 2) {
      postings[i][j] += postings[i][j - 2];
    }

    free(encoded_data);
  }
  
  // Merge postings.
  std::vector<std::pair<int32_t, float>> matched_documents;
  std::vector<int32_t> buffer_positions(num_keywords, 0);
  while (true) {
    int32_t min_doc = 1000*1000*10;
    for (int i = 0; i < num_keywords; i++) {
      if (buffer_positions[i] < keyword_posting_size[i]) {
        if (min_doc > postings[i][buffer_positions[i] * 2]) {
          min_doc = postings[i][buffer_positions[i] * 2];
        }
      }
    }

    if (min_doc == 1000*1000*10) break;
          
    float ranking_score = 0.0;

    for (int i = 0; i < num_keywords; i++) {
      if (min_doc == postings[i][buffer_positions[i] * 2]) {
        ranking_score += (log(1.0 + 1.0 * postings[i][buffer_positions[i] * 2 + 1])
            * log(NUM_TOTAL_DOCUMENTS / (1.0 * keyword_doc_frequency[i])));
        buffer_positions[i]++;
      }
    }

    matched_documents.emplace_back(std::make_pair(min_doc, ranking_score));
  }

  // Sort matched documents.
  std::sort(matched_documents.begin(), matched_documents.end(), sortByScore);
  for (int i = 0; i < 20 && i < matched_documents.size(); i++) {
    printf_fmt("%d %.2f\n", matched_documents[i].first, matched_documents[i].second);
  }

  printf_fmt("Total matched documents: %d ", matched_documents.size());
  printf_fmt("Num of keywords: %d ", num_keywords);
  printf_fmt("Total data fetched: %lld\n", total_data_fetched);

  // Clean.

  for (int i = 0; i < num_keywords; i++) {
    free(postings[i]);
  }

  free(postings);

  if (sgx_fclose(index_fd)) {
    printf_fmt("Error in closing the index file.\n");
  }
}

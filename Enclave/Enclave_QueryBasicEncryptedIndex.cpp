#include "Enclave.h"
#include "Enclave_t.h"

#include "PostingList.h"

#include <algorithm>
#include <cmath>
#include <vector>


bool sortByScore(const std::pair<int32_t, double> &a,
                 const std::pair<int32_t, double> &b)
{
  return (a.second > b.second);
}

std::vector<std::pair<int32_t, double>>
    merge_postings_by_wave(std::vector<PostingList*>& postings) {
  int num_keywords = postings.size();

  std::vector<std::pair<int32_t, double>> matched_documents;
 
  while (true) {
    int32_t min_doc = 1000*1000*10;
    for (int i = 0; i < num_keywords; i++) {
      if (postings[i]->getCurrentDocId() != -1
          && min_doc > postings[i]->getCurrentDocId()) {
        min_doc = postings[i]->getCurrentDocId();
      }
    }

    if (min_doc == 1000*1000*10) break;
          
    double ranking_score = 0.0;

    for (int i = 0; i < num_keywords; i++) {
      if (min_doc == postings[i]->getCurrentDocId()) {
        ranking_score += postings[i]->getCurrentDocScore();
        postings[i]->next();
      }
    }

    matched_documents.emplace_back(std::make_pair(min_doc, ranking_score));
  }

  std::sort(matched_documents.begin(), matched_documents.end(), sortByScore);

  return matched_documents;
}

void ecall_query_basic_encrypted_index(uint8_t* keyword_data, uint64_t keyword_data_size,
                                       const char* encrypted_index_file_name,
                                       const char* dataset_name, const char* merge_method) {
  // trec45 / clueweb.
  const double NUM_TOTAL_DOCUMENTS = !strcmp(dataset_name, "trec45") ? 528155 : 33836981; 

  int64_t *cast_keyword_data = (int64_t *)keyword_data;
  const int num_keywords = keyword_data_size * sizeof(uint8_t) / sizeof(int64_t) / 3;
  std::vector<int64_t> keyword_offset;
  std::vector<int64_t> keyword_data_length;
  std::vector<int64_t> keyword_doc_frequency;
  for (int i = 0; i < num_keywords; i++) {
    keyword_offset.push_back(cast_keyword_data[i * 3]);
    keyword_data_length.push_back(cast_keyword_data[i * 3 + 1]);
    keyword_doc_frequency.push_back(cast_keyword_data[i * 3 + 2]);
  }

  SGX_FILE *index_fd;
  index_fd = sgx_fopen_auto_key(encrypted_index_file_name, "r");
  if (!index_fd) {
    printf_fmt("Error in opening file.\n");
    return;
  }

  // Extract postings.
  std::vector<PostingList*> postings;
  for (int i = 0; i < num_keywords; i++) {
    postings.push_back(new PostingList(
        NUM_TOTAL_DOCUMENTS, keyword_doc_frequency[i], /*block_size=*/100));
    if (!postings.back()->tryInit(index_fd,
                                  keyword_offset[i],
                                  keyword_data_length[i])) {
      printf_fmt("Error in extracting postings.\n");
      return;
    }
  }
  
  // Merge and sort postings.
  std::vector<std::pair<int32_t, double>> matched_documents;
  if (!strcmp(merge_method, "wave")) {
    matched_documents = merge_postings_by_wave(postings);
  }

  for (int i = 0; i < 20 && i < matched_documents.size(); i++) {
    printf_fmt("%d %.2f\n", matched_documents[i].first, matched_documents[i].second);
  }

  printf_fmt("Total matched documents: %d ", matched_documents.size());
  printf_fmt("Num of keywords: %d ", num_keywords);
  printf_fmt("Total data fetched: %lld\n", -1);

  // Clean.
  for (int i = 0; i < num_keywords; i++) delete postings[i];

  if (sgx_fclose(index_fd)) {
    printf_fmt("Error in closing the index file.\n");
  }
}

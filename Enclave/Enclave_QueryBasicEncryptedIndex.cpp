#include "Enclave.h"
#include "Enclave_t.h"

#include "PostingList.h"

#include <algorithm>
#include <cmath>
#include <vector>

static const int kTopK = 20;

bool sortByCurrentDoc(PostingList* a, PostingList* b)
{
  return (a->getCurrentDocId() < b->getCurrentDocId());
}

bool sortByScore(const std::pair<int32_t, double> &a,
                 const std::pair<int32_t, double> &b)
{
  return (a.second > b.second);
}

std::vector<std::pair<int32_t, double>>
    merge_and_sort_postings_by_wave(std::vector<PostingList*>& postings) {
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

std::vector<std::pair<int32_t, double>>
    merge_and_sort_postings_by_wand(std::vector<PostingList*>& postings) {
  int num_keywords = postings.size();

  std::vector<std::pair<int32_t, double>> matched_documents;

  // Get the first kTopK doc.
  while (matched_documents.size() < kTopK) {
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

    matched_documents.push_back(std::make_pair(min_doc, ranking_score));
  }

  // Min heap.
  std::make_heap(matched_documents.begin(), matched_documents.end(), sortByScore);

  // Weak-And retrieval.
  while (true) {
    std::sort(postings.begin(), postings.end(), sortByCurrentDoc);

    int pivot = 0;

    // Skip finished posting lists.
    while (pivot < postings.size() && postings[pivot]->getCurrentDocId() == -1) {
      pivot += 1;
    }
    // All postings lists are finished.
    if (pivot >= postings.size()) break; 

    double attempt_ranking_score = postings[pivot]->getMaxRankingScore();
    while (attempt_ranking_score <= matched_documents.front().second) {
      pivot += 1;
      if (pivot >= postings.size()) break;
      attempt_ranking_score += postings[pivot]->getMaxRankingScore();
    }

    // Cannot find a valid pivot. All kTopK doc are retrieved.
    if (pivot >= postings.size()) break;

    int32_t pivot_doc = postings[pivot]->getCurrentDocId();
    double pivot_ranking_score = postings[pivot]->getCurrentDocScore();
    postings[pivot]->next();
    for (int i = 0; i < pivot; i++) {
      postings[i]->moveToDoc(pivot_doc); // Then current doc >= pivot_doc.
      if (postings[i]->getCurrentDocId() == pivot_doc) {
        pivot_ranking_score += postings[i]->getCurrentDocScore();
        postings[i]->next();
      }
    }

    for (int i = pivot + 1; i < postings.size(); i++) {
      if (postings[i]->getCurrentDocId() == pivot_doc) {
        pivot_ranking_score += postings[i]->getCurrentDocScore();
        postings[i]->next();
      }
    }

    if (pivot_ranking_score > matched_documents.front().second) {
      std::pop_heap(
          matched_documents.begin(), matched_documents.end(), sortByScore);
      matched_documents.pop_back();
      matched_documents.push_back(
          std::make_pair(pivot_doc, pivot_ranking_score));
      std::push_heap(
          matched_documents.begin(), matched_documents.end(), sortByScore);
    }
  }

  std::sort_heap(
      matched_documents.begin(), matched_documents.end(), sortByScore);
  return matched_documents;
}

std::vector<std::pair<int32_t, double>>
    merge_and_sort_postings_by_bmw(std::vector<PostingList*>& postings) {
  int num_keywords = postings.size();

  std::vector<std::pair<int32_t, double>> matched_documents;

  // Get the first kTopK doc.
  while (matched_documents.size() < kTopK) {
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

    matched_documents.push_back(std::make_pair(min_doc, ranking_score));
  }

  // Min heap.
  std::make_heap(matched_documents.begin(), matched_documents.end(), sortByScore);

  while (true) {
    std::sort(postings.begin(), postings.end(), sortByCurrentDoc);

    int pivot = 0;

    // Skip finished posting lists.
    while (pivot < postings.size() && postings[pivot]->getCurrentDocId() == -1) {
      pivot += 1;
    }
    // All postings lists are finished.
    if (pivot >= postings.size()) break; 

    double attempt_ranking_score = postings[pivot]->getMaxRankingScore();
    while (attempt_ranking_score <= matched_documents.front().second) {
      pivot += 1;
      if (pivot >= postings.size()) break;
      attempt_ranking_score += postings[pivot]->getMaxRankingScore();
    }

    // Cannot find a valid pivot. All kTopK doc are retrieved.
    if (pivot >= postings.size()) break;

    int32_t pivot_doc = postings[pivot]->getCurrentDocId();
    for (int i = 0; i < pivot; i++) {
      postings[i]->moveToDocShallow(pivot_doc); // Move to the block.
    }

    // Modify pivot in case of the next one is the same doc.
    while (pivot + 1 < postings.size()
           && postings[pivot + 1]->getCurrentDocId() == pivot_doc) {
      pivot += 1;
    }

    // Check Block Max.
    double attempt_block_max = postings[pivot]->getBlockwiseMaxRankingScore();
    for (int i = 0; i < pivot; i++)
      attempt_block_max += postings[i]->getBlockwiseMaxRankingScore();

    if (attempt_block_max > matched_documents.front().second) {
      // Check exact score.
      double pivot_ranking_score = postings[pivot]->getCurrentDocScore();
      postings[pivot]->next();
      for (int i = 0; i < pivot; i++) {
        postings[i]->moveToDoc(pivot_doc); // Then current doc >= pivot_doc.
        if (postings[i]->getCurrentDocId() == pivot_doc) {
          pivot_ranking_score += postings[i]->getCurrentDocScore();
          postings[i]->next();
        }
      }

      if (pivot_ranking_score > matched_documents.front().second) {
        std::pop_heap(
            matched_documents.begin(), matched_documents.end(), sortByScore);
        matched_documents.pop_back();
        matched_documents.push_back(
            std::make_pair(pivot_doc, pivot_ranking_score));
        std::push_heap(
            matched_documents.begin(), matched_documents.end(), sortByScore);
      }
    } else { // The current block does not work.
      int32_t next_possible_doc = 10 * 1000 * 1000;
      for (int i = 0; i <= pivot; i++) {
        next_possible_doc = std::min(next_possible_doc,
            postings[i]->getCurrentBlockLastDocId());
      }
      next_possible_doc += 1;

      if (pivot + 1 < postings.size()) {
        next_possible_doc = std::min(next_possible_doc,
            postings[pivot + 1]->getCurrentDocId());
      }

      for (int i = 0; i <= pivot; i++) {
        postings[i]->moveToDoc(next_possible_doc);
      }

      if (pivot + 1 < postings.size()) {
        postings[pivot + 1]->moveToDoc(next_possible_doc);
      }
    }
  }

  std::sort_heap(
      matched_documents.begin(), matched_documents.end(), sortByScore);
  return matched_documents;
}

void ecall_query_basic_encrypted_index(
    uint8_t* keyword_data, uint64_t keyword_data_size,
    const char* encrypted_index_file_name,
    const char* dataset_name, const char* merge_method) {
  // The # of doc in trec45 / clueweb.
  const double NUM_TOTAL_DOCUMENTS =
      !strcmp(dataset_name, "trec45") ? 528155 : 33836981; 

  // Unpack arguments.
  int64_t *cast_keyword_data = (int64_t *)keyword_data;
  const int num_keywords =
      keyword_data_size * sizeof(uint8_t) / sizeof(int64_t) / 3;
  std::vector<int64_t> keyword_offset,
                       keyword_data_length,
                       keyword_doc_frequency;
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
    if (!postings.back()->tryInit(
        index_fd, keyword_offset[i], keyword_data_length[i])) {
      printf_fmt("Error in extracting postings.\n");
      return;
    }
  }
  
  // Merge and sort postings.
  std::vector<std::pair<int32_t, double>> matched_documents;
  if (!strcmp(merge_method, "wave")) {
    matched_documents = merge_and_sort_postings_by_wave(postings);
  } else if (!strcmp(merge_method, "wand")) {
    matched_documents = merge_and_sort_postings_by_wand(postings);
  } else if (!strcmp(merge_method, "bmw")) {
    matched_documents = merge_and_sort_postings_by_bmw(postings);
  }

  for (int i = 0; i < kTopK && i < matched_documents.size(); i++)
    printf_fmt("%d %.2f\n",
               matched_documents[i].first, matched_documents[i].second);

  printf_fmt("Total matched documents: %d ", matched_documents.size());
  printf_fmt("Num of keywords: %d ", num_keywords);
  printf_fmt("Total data fetched: %lld\n", -1);

  // Clean.
  for (int i = 0; i < num_keywords; i++) delete postings[i];

  if (sgx_fclose(index_fd)) {
    printf_fmt("Error in closing the index file.\n");
  }
}

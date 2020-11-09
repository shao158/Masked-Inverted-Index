#include "PostingList.h"

#include <cmath>

#include "Simple9.h"

PostingList::PostingList(int64_t num_total_doc,
                         int64_t doc_frequency,
                         int64_t block_size)
    : curr_doc_id(-1),
      num_block(-1),
      posting_len(-1),
      BLOCK_SIZE(block_size),
      NUM_TOTAL_DOC(num_total_doc),
      DOC_FREQUENCY(doc_frequency) {}

PostingList::~PostingList() {
  if (data_buffer != nullptr) free(data_buffer);
}

bool PostingList::tryInit(SGX_FILE *index_fd,
                          int64_t data_offset,
                          int64_t data_length) {
  // Load data.
  if (!index_fd) {
    printf_fmt("Error in a null fd.\n");
    return false;
  }

  if (sgx_fseek(index_fd, data_offset, SEEK_SET)) {
    printf_fmt("Error in fseek.\n");
    return false;
  }

  uint8_t *encoded_data;
  encoded_data = (uint8_t *) malloc(data_length * sizeof(uint8_t));
  size_t size_read = sgx_fread(encoded_data, sizeof(uint8_t), data_length, index_fd);
  if (size_read != sizeof(uint8_t) * data_length) {
    printf_fmt("Error in fread.\n");
    return false;
  }
    
  // This decoding process can be non-oblivious.

  size_t size_data;
  data_buffer = NULL;
  simple9_decode(&data_buffer, &size_data, encoded_data);

  // Restore doc_id from delta encoding.
  for (size_t i = 2; i < size_data; i += 2) {
    data_buffer[i] += data_buffer[i - 2];
  }

  free(encoded_data); 

  curr_pos = 0;
  curr_doc_id = data_buffer[curr_pos];
    
  // Each doc has two uint32_t, doc_id and term_frequency.
  posting_len = size_data / 2;
    
  num_block = (posting_len + BLOCK_SIZE - 1) / BLOCK_SIZE;

  curr_block_idx = 0;
  for (int i = 0; i < posting_len; i++) {
    double tmp_score = (log(1.0 + 1.0 * data_buffer[i * 2 + 1])
        * log(NUM_TOTAL_DOC / (1.0 * DOC_FREQUENCY)));

    max_score = std::max(max_score, tmp_score);
    if (i % BLOCK_SIZE == 0) block_max_score.emplace_back(0);
    block_max_score[i / BLOCK_SIZE] = std::max(block_max_score[i / BLOCK_SIZE], tmp_score);
  }

  curr_block_max_score = block_max_score[0];

  return true;
}

double PostingList::getCurrentDocScore() const {
  if (curr_doc_id == -1) return -1;
  return (log(1.0 + 1.0 * data_buffer[curr_pos * 2 + 1])
      * log(NUM_TOTAL_DOC / (1.0 * DOC_FREQUENCY)));
}

int32_t PostingList::getCurrentBlockLastDocId() const {
  if (curr_block_idx + 1 > num_block) { // end, cannot be -1.
    return 100 * 1000 * 1000;
  } else if (curr_block_idx + 1 == num_block) { // last block;
    return data_buffer[(posting_len - 1) * 2];
  } else {
    return data_buffer[((curr_block_idx + 1) * BLOCK_SIZE - 1) * 2];
  }
}

bool PostingList::next() {
  if (curr_pos + 1 < posting_len) 
  { 
    curr_pos += 1;
    curr_doc_id = data_buffer[curr_pos * 2];
    curr_block_idx = curr_pos / BLOCK_SIZE;
    curr_block_max_score = block_max_score[curr_block_idx];
    return true;
  }
  else
  {
    curr_pos = posting_len;
    curr_doc_id = -1;
    curr_block_idx = num_block;
    curr_block_max_score = -1;
    return false;
  }
}

bool PostingList::moveToDocShallow(int32_t did) {
  if (curr_doc_id == -1) return false;

  if (curr_doc_id == did) return true;

  while (curr_block_idx < num_block && did > getCurrentBlockLastDocId()) {
    curr_block_idx += 1;
  }

  if (curr_block_idx < num_block) {
    curr_pos = curr_block_idx * BLOCK_SIZE;
    curr_doc_id = data_buffer[curr_pos * 2];
    curr_block_max_score = block_max_score[curr_block_idx];
    return true;
  } else {
    curr_pos = posting_len;
    curr_doc_id = -1;
    curr_block_idx = num_block;
    curr_block_max_score = -1;
    return false;
  }
}

bool PostingList::moveToDoc(int32_t did) {
  if (curr_doc_id == -1) return false;

  if (curr_doc_id == did) return true;

  while (curr_pos < posting_len && data_buffer[curr_pos * 2] < did) {
    curr_pos += 1;
  }

  if (curr_pos < posting_len) {
    curr_doc_id = data_buffer[curr_pos * 2];
    curr_block_idx = curr_pos / BLOCK_SIZE;
    curr_block_max_score = block_max_score[curr_block_idx];
    return true;
  } else {
    curr_doc_id = -1;
    curr_block_idx = num_block;
    curr_block_max_score = -1;
    return false;
  }
}

#include <vector>

#include "Enclave.h"
#include "Enclave_t.h"

class PostingList {
private:
  uint32_t* data_buffer;

  int32_t curr_pos;
  int32_t curr_doc_id;

  int32_t posting_len;

  // Gloabl maximum score.
  double max_score;

  int32_t num_block;
  int32_t curr_block_idx;
  double curr_block_max_score;

  std::vector<double> block_max_score;

  const int32_t BLOCK_SIZE;

  // The number of documents in this dataset.
  const double NUM_TOTAL_DOC;

  // The number of documents containing this term.
  // Can be different than posting_len
  // if the dataset is partially indexed.
  const double DOC_FREQUENCY;

public:
  explicit PostingList(int64_t num_total_doc,
                       int64_t doc_frequency,
                       int64_t block_size);

  PostingList(const PostingList&) = delete;
  PostingList& operator=(const PostingList&) = delete;

  ~PostingList();

  bool tryInit(SGX_FILE *index_fd,
               int64_t data_offset,
               int64_t data_length);

  inline double getMaxRankingScore() const
  {
    return max_score;
  }

  inline double getBlockwiseMaxRankingScore() const
  {
    return curr_block_max_score;
  }

  inline int32_t getCurrentDocId() const
  {
    return curr_doc_id;
  }

  double getCurrentDocScore() const;

  int32_t getCurrentBlockLastDocId() const;

  bool next();

  // Move to the block where the last doc id
  // is larger than (no matched doc id)
  // or equal to the input doc id. 
  bool moveToDocShallow(int32_t did);

  bool moveToDoc(int32_t did);
};  

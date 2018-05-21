#ifndef USER_CASE
#define USER_CASE

#define SEED_LENGTH 65
const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

int calculateDestRank(char *word, int length, int num_ranks)
{
    uint64_t hash = 0;
    
    for (uint64_t i = 0; i < length; i++)
    {
        uint64_t num_char = (uint64_t)word[i];
        uint64_t seed     = (uint64_t)key_seed_num[(i % SEED_LENGTH)];
        
        hash += num_char * seed * (i + 1);
    }
    
    return (int)(hash % (uint64_t)num_ranks);
}

typedef struct KEYVAL{      //changed
    int key_len;
    int val;
    char key[20];
} KEYVAL;


// Define letter and digit recognition

bool isLetter(char x)
{
  bool isletter=0;
  // printf("%u\n", x);
  if ((((unsigned int) x > 96) && ((unsigned int) x < 123 )) || (((unsigned int) x > 64) && ((unsigned int) x < 91))) isletter=1;
  return(isletter);
}

bool isDigit(char x)
{
  bool isdigit=0;
  if (((unsigned int) x > 47) && ((unsigned int) x < 58 )) isdigit=1;
  return(isdigit);
}

void Map(char* task, int task_len, int &task_count, KEYVAL *word)
{
    word->key_len=0;
    word->val=0;
    int i=task_count;
    while(!isDigit(task[i]) && !isLetter(task[i]) && i<task_len)
    {
      task_count++;
      i++;
    }
    if (isLetter(task[i]))
    {
    while (isLetter(task[i+word->key_len]) && i+word->key_len<task_len) word->key_len++;
    word->key =  new char[word->key_len];
    for(int j=0;j<word->key_len;j++) word->key[j]=task[i+j];
    }
    if (isDigit(task[i]))
    {
    while (isDigit(task[i+word->key_len]) && i+word->key_len<task_len) word->key_len++;
    word->key = new char[word->key_len];
    for(int j=0;j<word->key_len;j++) word->key[j]=task[i+j];
    }
    task_count+=word->key_len;
    word->val=1;
}

#endif

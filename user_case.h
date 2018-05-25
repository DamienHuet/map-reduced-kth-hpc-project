#include <stdint.h>
#include <vector>
#define WORD_LEN 20

#ifndef USER_CASE
#define USER_CASE

#define SEED_LENGTH 65
const char key_seed[SEED_LENGTH] = "b4967483cf3fa84a3a233208c129471ebc49bdd3176c8fb7a2c50720eb349461";
const unsigned short *key_seed_num = (unsigned short*)key_seed;

int calculateDestRank(char *word, int length, int num_ranks)
{
    uint64_t hash = 0;

    for (uint64_t i = 0; i < (uint64_t) length; i++)
    {
        uint64_t num_char = (uint64_t)word[i];
        uint64_t seed     = (uint64_t)key_seed_num[(i % SEED_LENGTH)];

        hash += num_char * seed * (i + 1);
    }

    return (int)(hash % (uint64_t)num_ranks);
}

typedef struct KEYVAL{
    int key_len = 0;
    int val;
    char key[WORD_LEN];
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

void Map(char* task, int task_len, int &task_count, int &key_len, char* key, int &val)
{
    key_len=0;
    val=0;
    int i=task_count;
    while(!isDigit(task[i]) && !isLetter(task[i]) && i<task_len)
    {
      task_count++;
      i++;
    }
    if (isLetter(task[i])){
        while (isLetter(task[i+key_len]) && i+key_len<task_len && key_len<WORD_LEN) key_len++;
        //word->key =  new char[word->key_len];
        for(int j=0;j<key_len;j++) key[j]=task[i+j];
    }
    if (isDigit(task[i])){
        while (isDigit(task[i+key_len]) && i+key_len<task_len && key_len<WORD_LEN) key_len++;
        //word->key = new char[word->key_len];
        for(int j=0;j<key_len;j++) key[j]=task[i+j];
    }
    task_count+=key_len;
    task_count+=1;
    val=1;
}

void Reduce(std::vector<KEYVAL> &vec, KEYVAL* ary, int cnt)
{
    int match, equal;
    for(int i=0;i<cnt;i++){
        match = 0;
        for(std::vector<KEYVAL>::iterator it=vec.begin();it!= vec.end();it++){
            if( (*it).key_len == (ary+i)->key_len){
                equal = 1;
                for(int j=0;j < (ary+i)->key_len;j++){
                    if( (*it).key[j] != (ary+i)->key[j]){
                        equal = 0;
                        break;
                    }
                }
                if(equal){
                    (*it).val++;
                    match = 1;
                    break;
                }
            }
        }
        if(!match){
            (ary+i)->val = 1;
            vec.push_back(*(ary+i));
        }
    }
}
#endif

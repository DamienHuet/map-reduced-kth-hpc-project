#ifndef USER_CASE
#define USER_CASE

typedef struct{
  int key_len;
  char key;
  int val;
} KEYVAL;


// Define letter and digit recognition

bool isLetter(char x)
{
  bool isletter=0;
  if (((int) x > 95) && ((int) x < 123 ) && ((int) x > 64) && ((int) x < 91)) isletter=1;
  return(isletter);
}

bool isDigit(char x)
{
  bool isdigit=0;
  if (((int) x > 47) && ((int) x < 58 )) isdigit=1;
  return(isdigit);
}

void Map(char* task,KEYVAL *word)
{
  int i=0;
  while(!isDigit(task[i]) || !isLetter(task[i])) i++;
  if (isLetter(task[i]))
  {
    word->key_len=0;
    while (isLetter(task[i+word->key_len])) word->key_len++;
    word->key=malloc(word->key_len*sizeof(char));
    for(int j=0;j<word->key_len;j++) word->key[j]=task[i+j];
  }
  if (isDigit(task[i]))
  {
    word.key_len=0;
    while (isDigit(task[i+j])) word.key_len++;
    word.key=malloc(word.key_len*sizeof(char));
    for(int j=0;j<word->key_len;j++) word.key[j]=task[i+j];
  }
  word.val=1;
  return(word);
}

#endif


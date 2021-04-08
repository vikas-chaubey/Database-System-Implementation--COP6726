#include "string"
#include "iostream"
#include "fstream"
#include <string.h>

/** This class defines all the utility functions which are used in the project code in different classes. **/
class Utilities {
    
    public:
    
        //This method checks whether a files is already present at the given path
        static int checkfileExist (const std::string& name) {
            if (FILE *file = fopen(name.c_str(), "r")) { fclose(file); return 1; }  
            else { return 0; }   
        }

                // This method generates a new file name with given extension
        static char* newRandomFileName(char* extension) {
            if ((extension == NULL) || (extension[0] == '\0')) { extension=""; }
            string str("temp_" + to_string(Utilities::getNextCounter()) + extension);
            char *cstr = new char[str.length() + 1];
            strcpy(cstr, str.c_str());
            return cstr;
        }

        // This method generates an Unique integer counter valur randomly 
        static int getNextCounter () {
            ifstream inputFileStream;
            ofstream outputFileStream;
            int counterVar = 0;
            if (checkfileExist("counter.txt")){
                inputFileStream.open("counter.txt",ios::in);
                inputFileStream.read((char*)&counterVar,sizeof(int));
                inputFileStream.close();
            }
            counterVar++;
            outputFileStream.open("counter.txt",ios::out);
            outputFileStream.write((char*)&counterVar,sizeof(int));
            outputFileStream.close();
            return counterVar;
        }   

};
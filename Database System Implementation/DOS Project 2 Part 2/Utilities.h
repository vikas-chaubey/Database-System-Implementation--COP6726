#include "string"
#include "iostream"

/**
 *  class for defining utility functions to be used
 *  across the project. Import "utility.h" to use it.
**/
class Utilities {
    
    public:
    
        // check if file exists already at the given file path
        static int checkfileExist (const std::string& name) {
            if (FILE *file = fopen(name.c_str(), "r")) {
                fclose(file);
                return 1;
            } else {
                return 0;
            }   
        }

};
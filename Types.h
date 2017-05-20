/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/

#ifndef TYPES_H_
#define TYPES_H_

#include <string>

using namespace std;

#define debug 0

typedef enum { COk, CError, CErrorOpenInputDir, CErrorOpenInputFile } TError;

void error(string message);

#endif /* TYPES_H_ */

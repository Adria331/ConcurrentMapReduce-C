/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"

#include <functional>
#include <string>
#include <mutex>


class MapReduce 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;
		
	vector<PtrMap> Mappers;
	vector<PtrReduce> Reducers;
	

	public:
		MapReduce(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers=2);
		TError Run();
		TError Partial(struct argument *arg);
		
	private:
		TError ThCreation(char *input);
		TError Map();
		TError Suffle();
		TError Reduce();
		
		
		inline void AddMap(PtrMap map) { Mappers.push_back(map); };
		inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;


#endif /* MAPREDUCE_H_ */

/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/

#include "MapReduce.h"
#include "Types.h"
#include "pthread.h"
#include <dirent.h> 
#include <string.h>
#include <typeinfo>
#include "Reduce.h"
#include <mutex>
using namespace std;
 
pthread_mutex_t mtx;

struct argument{
	PtrMap map;
	char input_path[256];				//ESTRUCTURA PEL SPLIT MAP I SHUFFLE
	PtrMapReduce mR;

};	


struct reduce{
	TReduce* reducer;					//ESTRUCTURA PEL REDUCE
};		



void * sms(struct argument * arg);				// CRIDA AL METODE DE CLASE PER FER SPLIT MAP I SHUFFLE
void * reducing(struct reduce * reduce);		// CRIDA AL METODE DE CLASE PER FER REDUCE
int nombreF(char* input); 						// OBTENIR NOMBRE FITXERS

MapReduce::MapReduce(char * input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers)
{
	MapFunction=mapf;
	ReduceFunction=reducef;
	InputPath = input;
	OutputPath = output;

	for(int x=0;x<nreducers;x++)
	{
		char filename[256];

		sprintf(filename, "%s/result.r%d", OutputPath, x+1);
		AddReduce(new TReduce(ReduceFunction, filename));
	}
}

int NombreF(char* input){  // Obtenir el nombre de fitxers dins d'input
	int numF=0;
	struct dirent *entry;
	DIR *dir;

	if ((dir=opendir(input))!=NULL)
	{
		while ((entry=readdir(dir))!=NULL){
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == 0x8 ) 
			{
				numF++;
			}
		}
		closedir(dir);
	}else{
		return 1;
	}
	return numF;
}

TError 
MapReduce::Run()
{
	if (ThCreation(InputPath)!=COk)
		error("MapReduce::Run-Error Split");
	
	if (Reduce()!=COk)
		error("MapReduce::Run-Error Reduce");
	
	return(COk);
}

void* sms(struct argument * arg){
	arg->mR->Partial(arg);   // MR = MapReduce 
}	

TError MapReduce::Partial(struct argument * arg){

	//////////////// SPLIT //////////////////////////////////////////////

	(arg->map)->ReadFileTuples(arg->input_path);

	/////////////////////////////////////////////////////////////////////
	////////////////  MAP  //////////////////////////////////////////////

	if ((arg->map)->Run()!=COk)
			error("MapReduce::Map Run error.\n");

	////////////////////////////////////////////////////////////////////
	/////////////// SHUFFLE ////////////////////////////////////////////
	
	TMapOuputIterator it2;
	multimap<string, int> output = (arg->map)->getOutput();

	for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
	{
		TMapOutputKey key = (*it1).first;
		pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

		int r = std::hash<TMapOutputKey>{}(key)%Reducers.size();

		if (debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);

		pthread_mutex_lock(&mtx);
		Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
		pthread_mutex_unlock(&mtx);

		for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
		   output.erase(it2);
	}
    	//output.erase(keyRange.first,keyRange.second);
    	//it2=keyRange.second;
    
	////////////////////////////////////////////////////////////////////

	return (COk);

}



void * reducing(struct reduce * reduce){
	reduce->reducer->Run();
}

TError 
MapReduce::Reduce()
{
	pthread_t th[Reducers.size()];
	struct reduce reduce[Reducers.size()];

	for(vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) 
	{
		reduce[m].reducer=Reducers[m];
		pthread_create(&th[m], NULL, (void* (*) (void*)) reducing, (void*) &reduce[m]); // un thread cada reduce de reducers
	}
	for (int i = 0; i<Reducers.size(); i++){
		pthread_join(th[i],NULL);
	}
	return(COk);
}



TError 
MapReduce::ThCreation(char *input) 
{	
	DIR *dir;
	struct dirent *entry;
	unsigned char isFile =0x8;
	struct dirent *SearchSize;
	pthread_mutex_init(&mtx, NULL);
	int i= NombreF(input);
	DIR *sas = opendir(input);

	if ((dir=opendir(input))!=NULL)    //Cas de tenir directori
	{
		pthread_t th[i];   //CREACIO DELS THREAD
		int j = 0;
		struct argument arg [i]; 

  		while ((entry=readdir(dir))!=NULL){
			
			if( strcmp(entry->d_name, ".")!=0 && strcmp(entry->d_name, "..")!=0 && entry->d_type == isFile ) 
			{
				arg[j].map = new TMap(MapFunction);
				AddMap(arg[j].map);					//Posem el map i el this (Mapreduce) a la estructura				
				arg[j].mR = this;
			    printf ("Processing input file %s\n", entry->d_name);
				sprintf(arg[j].input_path,"%s/%s",input, entry->d_name);
				pthread_create(&th[j], NULL, (void* (*) (void*))sms, (void*) &arg[j]);
				j++;	
			}
  		}
  		closedir(dir);

  		for(int z=0; z < i; z++){
			pthread_join(th[z], NULL);				// JOIN SPLIT MAP SHUFFLE
		} 	
	} 
	else 					//Cas de tenir 1 fitxer
	{
		
		if (errno==ENOTDIR)
		{	
			pthread_t th;
			struct argument arg;
			arg.map = new TMap(MapFunction); 		//en cas d'1 sol fitxer nomes 1 thread
			AddMap(arg.map);
			arg.mR = this;
			pthread_create(&th,NULL,(void *(*) (void *))sms, (void*) &arg);
			pthread_join(th, NULL);
		}
		else
		{
			error("MapReduce::Split - Error could not open directory");
			return(CErrorOpenInputDir);
		}
		
	}

	pthread_mutex_destroy(&mtx);

	return(COk);
}

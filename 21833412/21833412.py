from multiprocessing import Pool
import multiprocessing as mp
import time, random, sys

def main():
    email='correoprueba@unieuropea.com'
    password='12345'
    expediente=21833412
    print("********** UNIVERSIDAD EUROPEA DE MADRID **********")
    print("    Escuela de Ingenieria Arquitectura y Diseno")
    
    mail =raw_input('Ingresa tu correo electronico: ')
    
    contrasenia=raw_input('Ingresa tu password: ')
   
    if mail==email and contrasenia==password:
        print("********** MENU **********")
        print("1: Ejercicio A")
        print("2: Ejercicio B")
        case=input()
        '''switch(case,expediente)'''
        if case==1:
            merge_sort(expediente)
        elif case==2 :
            fibonacci(expediente)
    else:
        print("Datos incorrectos")
        print("Cerrando promgrama....")
    
'''def switch(case,expediente):
    sw = {
        1: merge_sort(expediente),
        2: fibonacci(expediente)
    }
    func = sw.get(case,"Empty")
    return func'''

def fibonacci(expediente):
    n_cores= mp.cpu_count()
    start = time.time() #almacena el tiempo en el que inicio

    '''pool = mp.Pool(processes = n_cores)
    result = pool.map(fib, expediente)

    print(result)'''
    end= time.time() #almacena el tiempo en el qeu termino
    tiempo_total = end - start
    print('Tiempo total de ejecucion de Fibonacci es: %f sec' % (tiempo_total))

def fib(n):
    if n <= 2:
        return 1
    else:
        return fib(n-1)+fib(n-2) 

def merge_sort(expediente):
    array=[random.randint(1,100) for x in range(expediente)]
    n_cores = mp.cpu_count()#numero de cores logicos a usar
    start = time.time() #almacena el tiempo en el que inicio

    array = mergeSortParallel(array, n_cores)

    end= time.time() #almacena el tiempo en el qeu termino
    tiempo_total = end - start 


    print('Tiempo total de ejecucion de MergeSort es: %f sec' % (tiempo_total))

    '''print('lista de elementos: ')
    for i in range(len(array)):
        print(array[i])'''

def merge(left, right):
    #devuelve los arrya juntos y ordenados
    ret = []
    li = ri = 0
    while li < len(left) and ri < len(right):
        if left[li] <= right[ri]:
            ret.append(left[li])
            li += 1
        else:
            ret.append(right[ri])
            ri += 1
    if li == len(left):
        ret.extend(right[ri:])
    else:
        ret.extend(left[li:])
    return ret

def mergesort(array):
    #regresa una copia ordenada del argumento 'array'
    if len(array) <= 1:
        return array
    ind = len(array)//2
    return merge(mergesort(array[:ind]), mergesort(array[ind:]))

def mergeWrap(AandB):
    #sirve de puente para hacer el merge de los subarray
    a,b = AandB
    return merge(a,b)

def mergeSortParallel(array, n_cores):
    numproc = n_cores
    #divide igualitariamente los indices del array
    endpoints = [int(x) for x in linspace(0, len(array), numproc+1)]
    args = [array[endpoints[i]:endpoints[i+1]] for i in range(numproc)]

    pool = Pool(processes = numproc)
    sortedsublists = pool.map(mergesort, args)

    #mientras exista mas de un subarray los va a juntar meintras sea posible
    while len(sortedsublists) > 1:
        args = [(sortedsublists[i], sortedsublists[i+1]) \
				for i in range(0, len(sortedsublists), 2)]
        sortedsublists = pool.map(mergeWrap, args)

    return sortedsublists[0]

def linspace(a,b,nsteps):
    """
    returns list of simple linear steps from a to b in nsteps.
    """
    ssize = float(b-a)/(nsteps-1)
    return [a + i*ssize for i in range(nsteps)]

if __name__ == '__main__':
    main()
# Finalizado

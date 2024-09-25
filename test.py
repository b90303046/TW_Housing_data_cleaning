#%%
#Example:
import pandas as pd 

DF0 = pd.DataFrame(data =dict(zip( ['col1','col2','col3'] ,[[1,2,3], [4,5,6], [3,3,6]]) ))

print(type(DF0))


def show_shape(func):
    def inner_fun(*args,**kwarg):
        print('執行函式{a}'.format(a = func.__name__))
        shape0 = args[0].shape
        state1 = '原來的dataframe的維度是{ans}'.format(ans = shape0)
        result = func(*args, **kwarg)
        shape1 = result.shape
        state2 = '經過調整後的dataframe維度是{ans}'.format(ans=shape1)
        result2 = (state1, state2)
        return result, result2
    return inner_fun


@show_shape
def drop_col(df, col):
    df1 = df.copy()
    df1.drop(col, axis=1, inplace=True)
    return df1



v = drop_col(DF0, col='col1')
print('\n\n\n')
print(v[1])
# #print(summ.__doc__) 測試註解 


# # Decorator 案例
# def document_it(func): # input 為一個函數
#     """
#     設計一個函數的運作介紹
#     """
#     def new_function(*args, **kwargs):
#         print('Running function:', func.__name__)
#         print('Positional arguments:', args) # 位置引數
#         print('Keyword arguments', kwargs)   # 關鍵字引數
#         result = func(*args, **kwargs)
#         print('Result:', result)
#         return result
#     return new_function  # output為一個函數


# def add_ints(a, b):
#     """
#     待裝飾的函數
#     """
#     return a + b

# #a = document_it(add_ints)(3,4)

# print('==========分隔線============')

# @document_it
# def multiply(a,b=2):
#     return a*b


# multiply(3,b = 4)


# def square_it(func):
#     """
#     """
#     def new_function(*args, **kwargs):
#         result = func(*args, **kwargs)
#         return result * result
#     return new_function

# @document_it
# @square_it
# def multiply2(a, b):
#     return a*b

# result = multiply2(3, b=4)
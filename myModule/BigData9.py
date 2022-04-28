# import pandas as pd
# import math as m
#
#
# df = pd.DataFrame({'age':['alice','bob','carol','dan','eve','frank','grace','heidi','ivan','judy','mallory'],
#                           'percentage':[70,36,95,63,43,84,54,15,21,91,34],
#                           'pass':['yes','no','yes','yes','no','yes','yes','no','no','yes','no']})
#
# totalNo = df[df['pass'] == 'no']
# totalYes = df[df['pass'] == 'yes']
#
# sumYes = totalYes['percentage'].sum()
# sumNo = totalNo['percentage'].sum()
#
# def calculateMean(sum, total):
#     mean = sum / total
#     return mean
#
# meanYes = calculateMean(sumYes, totalYes['pass'].count())
# print("Mean value for yes:",meanYes)
# meanNo = calculateMean(sumNo, totalNo['pass'].count())
# print("Mean value for No:",meanNo)
#
# listYes = totalYes['percentage'].tolist()
# listNo = totalNo['percentage'].tolist()
#
# def calculateVarience(mean, list, total):
#     totalVal = 0
#     for l in list:
#         val = (l - mean) ** 2
#         totalVal = totalVal + val
#     varience = totalVal / (total - 1)
#     return varience
#
# varienceYes = calculateVarience(meanYes, listYes, totalYes['pass'].count())
# print("Varience for Yes:",varienceYes)
# varienceNo = calculateVarience(meanNo, listNo, totalNo['pass'].count())
# print("Varience for No:",varienceNo)
#
# def probability(mean, varience, percentage):
#     p = (1 / (m.sqrt(varience * 2 * m.pi))) * pow(m.e,((-1/2)*pow((percentage - mean),2))/varience)
#     return p
#
# pYes = probability(meanYes, varienceYes, 61)
# print("P(61|yes):",pYes)
# pNo = probability(meanNo, varienceNo, 61)
# print("P(61|no):",pNo)
#
# #Final Result
#
# finalYes = (totalYes['pass'].count() / df['pass'].count()) * pYes
# print("P(yes|61):",finalYes)
# finalNo = (totalNo['pass'].count() / df['pass'].count()) * pNo
# print("P(no|61):",finalNo)


import pandas as pd
import math as m


df = pd.DataFrame({'type':['p','p','t','c','t','c','c','p','t','p'],
                   'amount':[9836.64,1864.28,181,181,865948.49,86120.87,5378.12,6027.23,539880.48,4300],
                   'old':[170136,21249,181,181,865948.49,94120.40,39005,175690.81,56395,4305.75],
                   'isFraud':['0','0','1','1','1','1','0','0','0','1']})

totalNo = df[df['isFraud'] == '0']
print(totalNo)
totalYes = df[df['isFraud'] == '1']

sumYes = totalYes['amount'].sum()
print(sumYes)
sumNo = totalNo['amount'].sum()

sumYesAge = totalYes['old'].sum()
sumNoAge = totalNo['old'].sum()


def calculateMean(sum, total):
    mean = sum / total
    return mean

meanYes = calculateMean(sumYes, totalYes['isFraud'].count())
print("Mean value for yes:",round(meanYes,2))
meanNo = calculateMean(sumNo, totalNo['isFraud'].count())
print("Mean value for No:",round(meanNo,2))

meanYesAge = calculateMean(sumYesAge, totalYes['old'].count())
print("Mean value for yes age:",round(meanYesAge,2))
meanNoAge = calculateMean(sumNoAge, totalNo['old'].count())
print("Mean value for No age:",round(meanNoAge,2))

listYes = totalYes['amount'].tolist()
listNo = totalNo['amount'].tolist()

listYesAge = totalYes['old'].tolist()
listNoAge = totalNo['old'].tolist()

def calculateVarience(mean, list, total):
    totalVal = 0
    for l in list:
        val = (l - mean) ** 2
        totalVal = totalVal + val
    std = m.sqrt(totalVal / (total - 1))
    print("Standard Deviation:", round(std,2))
    varience = totalVal / (total - 1)
    return varience

varienceYes = calculateVarience(meanYes, listYes, totalYes['amount'].count())
print("Varience for Yes:",round(varienceYes,2))
varienceNo = calculateVarience(meanNo, listNo, totalNo['amount'].count())
print("Varience for No:",round(varienceNo,2))

varienceYesAge = calculateVarience(meanYesAge, listYesAge, totalYes['old'].count())
print("Varience for Yes Age:",round(varienceYesAge,2))
varienceNoAge = calculateVarience(meanNoAge, listNoAge, totalNo['old'].count())
print("Varience for No Age:",round(varienceNoAge,2))

def probability(mean, varience, x):
    p = (1 / (m.sqrt(varience * 2 * m.pi))) * pow(m.e,((-1/2)*pow((x - mean),2))/varience)
    return p

pYesAge = probability(meanYesAge, varienceYesAge, 100)
print("P(38|1):",pYesAge)
pNoAge = probability(meanNoAge, varienceNoAge, 100)
print("P(38|0):",pNoAge)

pYes = totalYes.__len__()/df.__len__()
print("p(1):", pYes)
pNo = totalNo.__len__()/df.__len__()
print("p(0):", pNo)

pYesSalary = probability(meanYes, varienceYes, 7500)
print("P(71000|1):",pYesSalary)
pNoSalary = probability(meanNo, varienceNo, 7500)
print("P(71000|0):",pNoSalary)



pYesSalaryAge = pYes * pYesAge * pYesSalary * pYes * 0.3
print("P(1|38,71000):%s"%pYesSalaryAge)

pNoSalaryAge = pNo * pNoAge * pNoSalary * pNo * 0.17
print("P(0|38,71000):%s"%pNoSalaryAge)
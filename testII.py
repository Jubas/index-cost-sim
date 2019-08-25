import InvertedIndexCreator
import math
tcl = math.floor(131072 / 36)
#print(tcl)
lamb = 400
II = InvertedIndexCreator.InvertedIndexCreator(35484770, tcl, 30, 30, lamb)

qc, ii = II.create_index()

## lambda section
# pct diff between largest (approx. C-1) and avg size (target; EMMA paper; LARGE)
pct_diff = 1000000 / 2486


# sorted by size 
lc = sorted(qc, reverse=True)

# diff between largest (approx. C-1) and avg size (target)
actual_diff = lc[0] / II.pro_region

adjust_rate = 0.5
#while percentual difference is more than 2% (either plus/minus)
while (abs(actual_diff - pct_diff) > 2):
    if actual_diff - pct_diff < 0:
        lamb += adjust_rate
    else:
        lamb -= adjust_rate
    print(lamb)

    II = InvertedIndexCreator.InvertedIndexCreator(35484770, tcl, 30, 30, lamb)

    qc, ii = II.create_index()

    # sorted by size 
    lc = sorted(qc, reverse=True)
    print(lc[0])

    # pct diff between largest and avg size (target)
    actual_diff = lc[0] / II.pro_region
print("1st veri")
print(lc[0])
print(lamb)
# advocates 434.5 lambda


# pct diff between 2nd largest (approx. C-2) and avg size (target; EMMA paper; LARGE)
pct_diff = 900000 / 2486

#2nd lambda adjustment (approx. C-2)
while (abs(actual_diff - pct_diff) > 2):
    if actual_diff - pct_diff < 0:
        lamb += adjust_rate
    else:
        lamb -= adjust_rate
    print(lamb)

    II = InvertedIndexCreator.InvertedIndexCreator(35484770, tcl, 30, 30, lamb)

    qc, ii = II.create_index()

    # sorted by size 
    lc = sorted(qc, reverse=True)
    print(lc[1])

    # pct diff between 2th largest and avg size (target)
    actual_diff = lc[1] / II.pro_region
print(lamb)
# advocates 460.0 lambda


## verification section
# print(len(qc))
# print(len(ii))
# print("---")
# print(qc[int((len(qc)/2))])
# print(sum(ii[0]))
# n = 0
# for i in ii:
#    for j in i:
#        n += j
# print(n)
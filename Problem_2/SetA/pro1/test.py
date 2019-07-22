import matplotlib.pyplot as plt

a, b = [], []
a.append(1)
a.append(4)
b.append(3)
b.append(2)


plt.rcParams['animation.html'] = 'jshtml'
fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(a, b)
plt.show()

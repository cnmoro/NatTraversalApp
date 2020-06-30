import numpy as np
import matplotlib.pyplot as plt
import matplotlib

def plot_stacked_bar(data, series_labels, category_labels=None, 
                     show_values=False, value_format="{}", y_label=None, 
                     colors=None, grid=False, reverse=False):

    ny = len(data[0])
    ind = list(range(ny))

    axes = []
    cum_size = np.zeros(ny)

    data = np.array(data)

    if reverse:
        data = np.flip(data, axis=1)
        category_labels = reversed(category_labels)

    for i, row_data in enumerate(data):
        color = colors[i] if colors is not None else None
        axes.append(plt.bar(ind, row_data, bottom=cum_size, 
                            label=series_labels[i], color=color))
        cum_size += row_data
        
    if category_labels:
        plt.xticks(ind, category_labels)

    if y_label:
        plt.ylabel(y_label)

    plt.legend()

    if grid:
        plt.grid()

    if show_values:
        for axis in axes:
            for bar in axis:
                w, h = bar.get_width(), bar.get_height()
                plt.text(bar.get_x() + w/2, bar.get_y() + h/2, 
                         value_format.format(h), ha="center", 
                         va="center")

def prepare_plot(csv_data, nome_arq):
    category_labels = []
    a = []
    b = []

    csv_data = csv_data.split(';')
    for data in csv_data:
        data = data.split(',')
        category_labels.append(data[0].strip())
        a.append(float(data[1].strip()))
        b.append(float(data[2].strip()))

    alocal = []
    bremote = []

    for i in range(len(a)):
        if a[i] > b[i]:
            a[i] -= b[i]
            alocal.append(a[i])
            bremote.append(b[i])
        else:
            b[i] -= a[i]
            alocal.append(a[i])
            bremote.append(b[i])

    plt.figure(figsize=(6, 4))

    series_labels = ['Local', 'Remoto']

    data = [ alocal, bremote ]

    plot_stacked_bar(
        data, 
        series_labels, 
        category_labels=category_labels, 
        show_values=False, 
        value_format="{:.1f}",
        colors=['tab:orange', 'tab:green']
    )

    fig = matplotlib.pyplot.gcf()
    fig.set_size_inches(10, 5)
    plt.xticks(rotation=45)
    plt.xlabel('Proporção Local / Remoto', fontsize=11, labelpad=15)
    plt.ylabel('Tempo (seg)', fontsize=11, labelpad=15)
    
    plt.savefig(nome_arq, bbox_inches='tight')
    
# header: % Local/Remoto,Tempo Local,Tempo Remoto
csv_data = """ 
            0/100,0.0,25.39;
            10/90,2.81,32.0;
            20/80,5.98,25.08;
            30/70,8.37,36.2;
            40/60,13.42,33.19;
            50/50,14.63,28.49;
            60/40,18.17,20.85;
            70/30,18.28,17.01;
            80/20,17.26,15.32;
            90/10,10.18,5.41;
            100/0,6.25,0.0
        """

prepare_plot(csv_data, 'GRAFICO_TCC_COM_GARANTIA_RECEBIMENTO_DADOS.png')

# header: % Local/Remoto,Tempo Local,Tempo Remoto
csv_data = """
            0/100,0,13.71;
            10/90,3.17,14;
            20/80,3,10.43;
            30/70,7.63,7.52;
            40/60,9.52,8.71;
            50/50,7.24,5.29;
            60/40,4.26,6.67;
            70/30,6.95,3.31;
            80/20,7.02,2.49;
            90/10,6.85,1.28;
            100/0,6.25,0
        """

prepare_plot(csv_data, 'GRAFICO_TCC_SEM_GARANTIA_RECEBIMENTO_DADOS.png')
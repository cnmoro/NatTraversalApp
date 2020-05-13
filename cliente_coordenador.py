from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

import time
import sys
import os
from threading import Thread
import threading

from shapely.geometry import Point, Polygon

# https://www.keene.edu/campus/maps/tool/
coords_parana = [(-54.3603516, -24.5371294), (-54.3933105, -24.7468313), (-54.4757080, -24.9711199), (-54.4702148, -25.1800878), (-54.5251465, -25.3340967), (-54.5910645, -25.4035850), (-54.6350098, -25.5275717), (-54.5526123, -25.6365741), (-54.4592285, -25.7058875), (-54.3438721, -25.5870398), (-54.2065430, -25.5870398), (-54.1241455, -25.5176574), (-54.1241455, -25.5870398), (-54.0032959, -25.5969483), (-53.8989258, -25.6811373), (-53.8494873, -25.8888786), (-53.8385010, -26.0074242), (-53.7231445, -26.1357136), (-53.6682129, -26.1726940), (-53.6517334, -26.2343020), (-53.5528564, -26.2687883), (-53.4704590, -26.2737140), (-53.3770752, -26.2540097), (-53.2836914, -26.2835649), (-53.1738281, -26.3524979), (-53.1024170, -26.3820280), (-52.9815674, -26.3721854), (-52.8167725, -26.3623421), (-52.6684570, -26.3820280), (-52.5695801, -26.4263090), (-52.4047852, -26.4410656), (-52.1520996, -26.4656556), (-52.0916748, -26.4951568), (-51.9818115, -26.5639634), (-51.8170166, -26.5836148), (-51.6412354, -26.5787023), (-51.5039063, -26.5934393), (-51.4160156, -26.7063599), (-51.3720703, -26.6720045), (-51.2951660, -26.6523683), (-51.2127686, -26.5983512), (-51.2402344, -26.5099045), (-51.2786865, -26.4410656), (-51.2786865, -26.4017105), (-51.2347412, -26.3180365), (-51.1029053, -26.2343020), (-50.9985352, -26.2343020), (-50.8502197, -26.2737140), (-50.7348633, -26.2244469), (-50.6140137, -26.0863881), (-50.5480957, -26.0419774), (-50.3723145, -26.0715865), (-50.3393555, -26.1357136), (-50.2514648, -26.0617176), (-50.0482178, -26.0419774), (-49.9053955, -26.0469128), (-49.5922852, -26.2244469), (-49.4989014, -26.2244469), (-49.4110107, -26.1702290), (-49.1857910, -26.0172976), (-48.8122559, -25.9777990), (-48.6254883, -25.9827370), (-48.4606934, -25.6563820), (-48.3453369, -25.5672204), (-48.2849121, -25.5325285), (-48.0706787, -25.2446960), (-48.1915283, -25.1850589), (-48.1530762, -25.1403119), (-48.2244873, -25.0507688), (-48.2244873, -24.9960157), (-48.2684326, -24.9711199), (-48.3013916, -25.0433039), (-48.3233643, -25.0358386), (-48.4030151, -24.9910370), (-48.4167480, -24.9686301), (-48.4552002, -24.9860580), (-48.4991455, -25.0532570), (-48.5403442, -25.0855989), (-48.5705566, -25.0433039), (-48.6007690, -24.9960157), (-48.5705566, -24.9511996), (-48.5540771, -24.8640106), (-48.5760498, -24.8490577), (-48.5595703, -24.7942150), (-48.5073853, -24.7617965), (-48.5046387, -24.7293696), (-48.5952759, -24.6644904), (-48.6584473, -24.6769698), (-48.6776733, -24.6919434), (-48.6996460, -24.6570022), (-48.7600708, -24.6869524), (-48.8095093, -24.6769698), (-48.8534546, -24.6395279), (-48.9523315, -24.6669864), (-49.0045166, -24.6470172), (-49.0127563, -24.6220511), (-49.0567017, -24.6270447), (-49.0676880, -24.6619944), (-49.1638184, -24.6694823), (-49.2297363, -24.6919434), (-49.3011475, -24.6744740), (-49.3176270, -24.5471232), (-49.2901611, -24.5371294), (-49.2901611, -24.4996456), (-49.2462158, -24.4721504), (-49.1967773, -24.3395895), (-49.2846680, -24.2970405), (-49.3588257, -24.2093947), (-49.3286133, -24.1292086), (-49.4302368, -24.0790667), (-49.4604492, -24.0213793), (-49.4879150, -23.9912713), (-49.5263672, -23.9034159), (-49.6032715, -23.8506740), (-49.5620728, -23.8104753), (-49.5593262, -23.6847742), (-49.6170044, -23.6243946), (-49.6005249, -23.5337730), (-49.6115112, -23.4556877), (-49.5730591, -23.4204082), (-49.6636963, -23.1807636), (-49.7351074, -23.1201536), (-49.7460938, -23.0898384), (-49.9053955, -23.0443527), (-49.9438477, -22.9786240), (-50.0152588, -22.8723793), (-50.2130127, -22.9280417), (-50.3393555, -22.9280417), (-50.3942871, -22.8875622), (-50.4492188, -22.9229824), (-50.5371094, -22.9229824), (-50.6414795, -22.8825014), (-50.7183838, -22.8825014), (-50.7403564, -22.9280417), (-50.7952881, -22.9331007), (-50.8227539, -22.8571947), (-50.9106445, -22.7711166), (-51.0534668, -22.7508550), (-51.2622070, -22.7001879), (-51.3500977, -22.6495021), (-51.5093994, -22.6596408), (-51.6302490, -22.6545715), (-51.8060303, -22.6190816), (-51.9763184, -22.5632932), (-52.0477295, -22.5277798), (-52.1850586, -22.5024075), (-52.1850586, -22.6140109), (-52.2949219, -22.6140109), (-52.4597168, -22.6140109), (-52.5366211, -22.6038688), (-52.6025391, -22.5632932), (-52.7014160, -22.6140109), (-52.8552246, -22.5835825), (-53.0310059, -22.5632932), (-53.1518555, -22.6444325), (-53.3166504, -22.7559207), (-53.4924316, -22.8470707), (-53.6022949, -22.9381596), (-53.6572266, -23.0392977), (-53.6682129, -23.1908626), (-53.7561035, -23.3220800), (-53.8989258, -23.4330091), (-54.0087891, -23.4632463), (-54.0856934, -23.7752912), (-54.1186523, -23.9360549), (-54.3273926, -24.0865893), (-54.3713379, -24.1467536), (-54.3273926, -24.2669973), (-54.2834473, -24.3771208), (-54.3603516, -24.4771500), (-54.3603516, -24.5371294)]
parana = Polygon(coords_parana)

remote_start = 0

local_pronto = False
remoto_pronto = False

itens_enviados = {}
itens_recebidos = {}

semaforo = threading.Semaphore()

class ClientProtocol(DatagramProtocol):

    def esta_no_poligono(self, lon, lat):
        global poligono

        # Verifica se esta no poligono
        ponto = Point(lon, lat)
        
        # Se estiver, escrever resultado em arquivo
        if ponto.within(parana):
            self.escrever_resultado(lon, lat)

    def escrever_resultado(self, lon, lat):
        # Abrir arquivo .csv resultado
        with open("result.csv", "a+") as f:
            
            # Fazer append
            f.write(str(lon) + ';' + str(lat) + '\n')

    def escrever_resultado_recebido(self, lon, lat):
        if str(lon) != '-' and str(lat) != '':
            # Abrir arquivo .csv resultado
            with open("result_remote.csv", "a+") as f:
                
                # Fazer append
                f.write(str(lon) + ';' + str(lat) + '\n')

    def processar_arquivo_remotamente(self):#, linha_de_inicio):
        global itens_enviados
        global itens_recebidos
        global remote_start
        global local_pronto
        global remoto_pronto

        remote_start = time.time()
        falhas = []

        # Abrir arquivo .csv
        with open('test2.csv', 'r') as _file:

            # Pula linhas ate chegar na linha de inicio especificada
            # for i in range(0, linha_de_inicio):
            #     _file.readline()

            i = 0
            # Ler linha a linha
            while True:
                linha = _file.readline()
                i += 1

                # Se chegou ao final do arquivo, sair
                if not linha:
                    break

                dado = str(i) + ';' + str(linha.strip())

                try:
                    itens_enviados[i] = linha.strip()
                    self.transport.write(dado, self.peer_address)
                except:
                    falhas.append(dado)
            
        # Garante que todas as linhas sejam enviadas para o peer
        while len(falhas) > 0:
            linha = falhas.pop()

            try:
                self.transport.write(linha, self.peer_address)
                time.sleep(0.0005)
            except:
                falhas.append(linha)

        print 'enviou ultimo arquivo remoto!'

        # Garante que todos os dados enviados receberam uma resposta correspondente
        while True:
            semaforo.acquire()
            enviados_keys = itens_enviados.keys()
            recebidos_keys = itens_recebidos.keys()
            semaforo.release()
            matches = set(enviados_keys) - set(recebidos_keys)

            if not (set(enviados_keys) - set(recebidos_keys)):
                print("Lista enviada bate com lista recebida")
                break
            else:
                nao_recebidos = list(matches)
                for idx in nao_recebidos:
                    try:
                        data = itens_enviados[idx]
                        self.transport.write(str(idx) + ';' + data, self.peer_address)
                        time.sleep(0.0005)
                    except:
                        pass
        
        end = time.time()
        elapsed = end - remote_start
        print 'processamento remoto finalizado em ' + str(elapsed) + ' segundos'
        remoto_pronto = True
        if local_pronto:
            sys.exit()

    def processar_arquivo_localmente(self):#, linha_final):
        #global arquivo
        global remoto_pronto
        global local_pronto

        start = time.time()

        # Abrir arquivo .csv
        with open('local.csv', 'r') as _file:

            linhas = 0
            # Ler linha a linha
            while True:
                linha = _file.readline()
                
                # Se chegou ao final do arquivo
                # ou ja processou o num de linhas especificado, sair
                if not linha:
                    print 'terminou processamento local!'
                    break

                linha = linha.split(';')

                # Obter longitude e latitude
                self.esta_no_poligono(float(linha[0]), float(linha[1]))

                linhas += 1

                if linhas % 100000 == 0:
                    print 'processou ' + str(linhas)

        end = time.time()
        elapsed = end - start
        print 'processamento local finalizado em ' + str(elapsed) + ' segundos'
        local_pronto = True

        if remoto_pronto:
            sys.exit()

    def dividir_tarefa_processar(self):
        #global arquivo
        
        # Obter quantidade de linhas no arquivo para processar
        # stream = os.popen('wc -l ' + arquivo)
        # output = stream.read()
        # qtd_linhas = int(output.split(' ')[0])

        # Definir parcela de divisao para processamento local e remoto
        # parcela_local = 0.4 # 60%

        # Abrir duas threads, uma para processamento local e outra remoto
        # Thread local -> executar lendo arquivo do inicio
        # linha_final = qtd_linhas * parcela_local
        thread_proc_local = Thread(target=self.processar_arquivo_localmente)#, kwargs=dict(linha_final=int(linha_final)))
        thread_proc_local.start()

        # Thread remota -> executar lendo arquivo pulando as X linhas iniciais
        # linha_de_inicio = qtd_linhas * parcela_local + 1
        thread_proc_remoto = Thread(target=self.processar_arquivo_remotamente)#, kwargs=dict(linha_de_inicio=int(linha_de_inicio)))
        thread_proc_remoto.start()
        
    def startProtocol(self):
        self.server_connect = False
        self.peer_init = False
        self.peer_connect = False
        self.peer_address = None
        self.connected_success = False
        self.transport.write('0', (sys.argv[1], int(sys.argv[2])))

    def toAddress(self, data):
        ip, port = data.split(':')
        return (ip, int(port))

    def datagramReceived(self, datagram, host):
        global remote_start
        global remoto_pronto
        global local_pronto
        global itens_enviados
        global itens_recebidos

        if not self.server_connect:
            self.server_connect = True
            self.transport.write('ok', (sys.argv[1], int(sys.argv[2])))
            print 'Connected to server, waiting for peer...'

        elif not self.peer_init:
            self.peer_init = True
            self.peer_address = self.toAddress(datagram)
            self.transport.write('init', self.peer_address)
            self.transport.write('init', self.peer_address)
            print 'Sent init to %s:%d' % self.peer_address

        elif not self.peer_connect:
            self.peer_connect = True
            host = self.transport.getHost().host
            port = self.transport.getHost().port
            msg = 'Message from %s:%d' % (host, port)
            self.transport.write(msg, self.peer_address)

        else:
            if not self.connected_success:
                self.connected_success = True

                time.sleep(1)
                self.dividir_tarefa_processar()
            else:
                try:
                    resultado = datagram.split(';')
                    idx = resultado[0]

                    semaforo.acquire()
                    itens_recebidos[int(idx)] = datagram
                    self.escrever_resultado_recebido(resultado[1], resultado[2])
                    semaforo.release()
                except:
                    pass

if __name__ == '__main__':

    sys.argv.append('54.39.99.92')
    #sys.argv.append('192.168.30.183')
    sys.argv.append('8089')

    if len(sys.argv) < 3:
        print "Uso: $ python client.py SERVER_IP SERVER_PORT"
        sys.exit(1)

    protocol = ClientProtocol()
    t = reactor.listenUDP(0, protocol)
    reactor.run()

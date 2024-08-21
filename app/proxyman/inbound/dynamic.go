package inbound

import (
        "context"
        "sync"

        core "github.com/v2fly/v2ray-core/v5"
        "github.com/v2fly/v2ray-core/v5/app/proxyman"
        "github.com/v2fly/v2ray-core/v5/common/dice"
        "github.com/v2fly/v2ray-core/v5/common/mux"
        "github.com/v2fly/v2ray-core/v5/common/net"
        "github.com/v2fly/v2ray-core/v5/proxy"
        "github.com/v2fly/v2ray-core/v5/transport/internet"
)

type DynamicInboundHandler struct {
        tag            string
        v              *core.Instance
        proxyConfig    interface{}
        receiverConfig *proxyman.ReceiverConfig
        streamSettings *internet.MemoryStreamConfig
        portMutex      sync.Mutex
        portsInUse     map[net.Port]bool
        workerMutex    sync.RWMutex
        workers        []worker
        mux            *mux.Server
        ctx            context.Context
}

func NewDynamicInboundHandler(ctx context.Context, tag string, receiverConfig *proxyman.ReceiverConfig, proxyConfig interface{}) (*DynamicInboundHandler, error) {
        v := core.MustFromContext(ctx)
        h := &DynamicInboundHandler{
                tag:            tag,
                proxyConfig:    proxyConfig,
                receiverConfig: receiverConfig,
                portsInUse:     make(map[net.Port]bool),
                mux:            mux.NewServer(ctx),
                v:              v,
                ctx:            ctx,
        }

        mss, err := internet.ToMemoryStreamConfig(receiverConfig.StreamSettings)
        if err != nil {
                return nil, newError("failed to parse stream settings").Base(err).AtWarning()
        }
        if receiverConfig.ReceiveOriginalDestination {
                if mss.SocketSettings == nil {
                        mss.SocketSettings = &internet.SocketConfig{}
                }
                if mss.SocketSettings.Tproxy == internet.SocketConfig_Off {
                        mss.SocketSettings.Tproxy = internet.SocketConfig_Redirect
                }
                mss.SocketSettings.ReceiveOriginalDestAddress = true
        }

        h.streamSettings = mss

        h.allocateInitialWorkers()

        return h, nil
}

func (h *DynamicInboundHandler) allocatePort() net.Port {
        from := int(h.receiverConfig.PortRange.From)
        delta := int(h.receiverConfig.PortRange.To) - from + 1

        h.portMutex.Lock()
        defer h.portMutex.Unlock()

        for {
                r := dice.Roll(delta)
                port := net.Port(from + r)
                _, used := h.portsInUse[port]
                if !used {
                        h.portsInUse[port] = true
                        return port
                }
        }
}

func (h *DynamicInboundHandler) closeWorkers(workers []worker) {
        portsToDel := make([]net.Port, len(workers))
        for idx, worker := range workers {
                portsToDel[idx] = worker.Port()
                if err := worker.Close(); err != nil {
                        newError("failed to close worker").Base(err).WriteToLog()
                }
        }

        h.portMutex.Lock()
        for _, port := range portsToDel {
                delete(h.portsInUse, port)
        }
        h.portMutex.Unlock()
}

func (h *DynamicInboundHandler) allocateInitialWorkers() {
        concurrency := h.receiverConfig.AllocationStrategy.GetConcurrencyValue()
        workers := make([]worker, 0, concurrency)

        address := h.receiverConfig.Listen.AsAddress()
        if address == nil {
                address = net.AnyIP
        }

        uplinkCounter, downlinkCounter := getStatCounter(h.v, h.tag)

        for i := uint32(0); i < concurrency; i++ {
                port := h.allocatePort()
                rawProxy, err := core.CreateObject(h.v, h.proxyConfig)
                if err != nil {
                        newError("failed to create proxy instance").Base(err).AtWarning().WriteToLog()
                        continue
                }
                p := rawProxy.(proxy.Inbound)
                nl := p.Network()
                if net.HasNetwork(nl, net.Network_TCP) {
                        worker := &tcpWorker{
                                tag:             h.tag,
                                address:         address,
                                port:            port,
                                proxy:           p,
                                stream:          h.streamSettings,
                                recvOrigDest:    h.receiverConfig.ReceiveOriginalDestination,
                                dispatcher:      h.mux,
                                sniffingConfig:  h.receiverConfig.GetEffectiveSniffingSettings(),
								uplinkCounter:   uplinkCounter,
                                downlinkCounter: downlinkCounter,
                                ctx:             h.ctx,
                        }
                        if err := worker.Start(); err != nil {
                                newError("failed to create TCP worker").Base(err).AtWarning().WriteToLog()
                                continue
                        }
                        workers = append(workers, worker)
                }

                if net.HasNetwork(nl, net.Network_UDP) {
                        worker := &udpWorker{
                                ctx:             h.ctx,
                                tag:             h.tag,
                                proxy:           p,
                                address:         address,
                                port:            port,
                                dispatcher:      h.mux,
                                sniffingConfig:  h.receiverConfig.GetEffectiveSniffingSettings(),
                                uplinkCounter:   uplinkCounter,
                                downlinkCounter: downlinkCounter,
                                stream:          h.streamSettings,
                        }
                        if err := worker.Start(); err != nil {
                                newError("failed to create UDP worker").Base(err).AtWarning().WriteToLog()
                                continue
                        }
                        workers = append(workers, worker)
                }
        }

        h.workerMutex.Lock()
        h.workers = workers
        h.workerMutex.Unlock()
}

func (h *DynamicInboundHandler) Start() error {
        return nil // No periodic task is needed
}

func (h *DynamicInboundHandler) Close() error {
        h.workerMutex.Lock()
        defer h.workerMutex.Unlock()

        h.closeWorkers(h.workers)
        return nil
}

func (h *DynamicInboundHandler) GetRandomInboundProxy() (interface{}, net.Port, int) {
        h.workerMutex.RLock()
        defer h.workerMutex.RUnlock()

        if len(h.workers) == 0 {
                return nil, 0, 0
        }
        w := h.workers[dice.Roll(len(h.workers))]
        return w.Proxy(), h.allocatePort(), 0
}

func (h *DynamicInboundHandler) Tag() string {
        return h.tag
}

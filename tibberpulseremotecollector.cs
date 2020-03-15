using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace tibberpulseremotecollector {

    public class TibberPulseConfigSection : ConfigurationSection {
        [ConfigurationProperty("authToken", IsRequired = true)]
        public string AuthToken => this["authToken"] as string;

        [ConfigurationProperty("homeId", IsRequired = true)]
        public string HomeId => this["homeId"] as string;

        [ConfigurationProperty("influxHost", IsRequired = true)]
        public string InfluxHost => this["influxHost"] as string;

        [ConfigurationProperty("influxDatabase", IsRequired = true)]
        public string InfluxDatabase => this["influxDatabase"] as string;

        [ConfigurationProperty("influxMeasurement", IsRequired = true)]
        public string InfluxMeasurement => this["influxMeasurement"] as string;
    }


    public class Program {
#pragma warning disable CS0649
        class PulseData<T> where T : PayloadData {
            public string type;
            public string id;
            public Payload<T> payload;
        }

        class Payload<T> {
            public T data;
        }

        abstract class PayloadData { }

        class LiveMeasurementData : PayloadData {
            public LiveMeasurement liveMeasurement;
        }

        class LiveMeasurement {
            public string timestamp;
            public double? power;
            public double? powerProduction;
            public double? accumulatedConsumption;
            public double? accumulatedProduction;
            public double? lastMeterConsumption;
            public double? lastMeterProduction;
            public double? powerFactor;
            public double? voltagePhase1;
            public double? voltagePhase2;
            public double? voltagePhase3;
            public double? currentPhase1;
            public double? currentPhase2;
            public double? currentPhase3;
        }
#pragma warning restore CS0649

        class InfluxWriter {
            static readonly HttpClient client = new HttpClient();
            readonly string measurement;
            readonly string server;
            readonly string database;
            static readonly CultureInfo decimaldotculture = CultureInfo.InvariantCulture;
            static readonly long unixbase = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).ToFileTime();
            public InfluxWriter(string server, string database, string measurement)
            {
                this.measurement = measurement;
                this.server = server;
                this.database = database;
            }

            private static string influxescape(string data)
            {
                return data.Replace("\\", "\\\\").Replace(" ", "\\ ").Replace(",", "\\,").Replace("=", "\\=");
            }

            private static string influxvalue(object value)
            {
                if (value is int i) {
                    return $"{i}i";
                } else if (value is double d) {
                    return d.ToString("F", decimaldotculture.NumberFormat);
                } else {
                    return $"\"{value.ToString().Replace("\"", "\\\"")}\"";
                }
            }
            public void Write(DateTime timestamp, Dictionary<string, object> values)
            {
                string linedata = $"{measurement} {string.Join(",", values.Select(kvp => $"{influxescape(kvp.Key)}={influxvalue(kvp.Value)}"))} {(timestamp.ToFileTime() - unixbase) / 10000}";
                HttpContent content = new StringContent(linedata);
                client.PostAsync($"http://{server}:8086/write?db={database}&precision=ms", content);
            }
        }

        public static async Task Main(string[] args)
        {
            Configuration execonfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            TibberPulseConfigSection config = execonfig.GetSection("tibberPulse") as TibberPulseConfigSection;

            if(string.IsNullOrEmpty(config.InfluxDatabase) || string.IsNullOrEmpty(config.InfluxHost) || string.IsNullOrEmpty(config.InfluxMeasurement) ) {
                Console.WriteLine("Invalid database parameters");
                return;
            }
            if (string.IsNullOrEmpty(config.AuthToken) || string.IsNullOrEmpty(config.HomeId)) {
                Console.WriteLine("Invalid pulse parameters");
                return;
            }

            InfluxWriter influx = new InfluxWriter(config.InfluxHost, config.InfluxDatabase, config.InfluxMeasurement);

            var cancelall = new CancellationTokenSource();
            bool softexiting = false;
            bool receiving = false;
            int curid = 0;
            ClientWebSocket ws = null;

            Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) {
                e.Cancel = true;
                softexiting = true;
                cancelall.CancelAfter(10000);
                string message = JsonConvert.SerializeObject(new { type = "stop", id = curid.ToString() });
                Console.WriteLine(message);
                var buffer = Encoding.UTF8.GetBytes(message);
                if (receiving)
                    try {
                        ws?.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, cancelall.Token);
                    }
                    catch (WebSocketException wse) {
                        Console.WriteLine($"WebSocketException when stopping {wse.Message}");
                    }
            };

            CancellationTokenSource timeout;

            while (!softexiting)
                using (timeout = CancellationTokenSource.CreateLinkedTokenSource(cancelall.Token))
                using (ws = new ClientWebSocket())
                    try {
                        ws.Options.AddSubProtocol("graphql-ws");
                        ws.Options.SetRequestHeader("Authorization", config.AuthToken);
                        // Timeout for connecting and registering = 30s
                        timeout.CancelAfter(30000);
                        await ws.ConnectAsync(new Uri("wss://api.tibber.com/v1-beta/gql/subscriptions"), timeout.Token);
                        string message;
                        byte[] buffer;
                        const int MAXLEN = 1024 * 16;
                        byte[] inbuffer = new byte[MAXLEN];
                        WebSocketReceiveResult result;
                        string json;

                        message = JsonConvert.SerializeObject(new { type = "connection_init" });
                        Console.WriteLine(message);
                        buffer = Encoding.UTF8.GetBytes(message);
                        await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, timeout.Token);

                        result = await ws.ReceiveAsync(new ArraySegment<byte>(inbuffer), timeout.Token);
                        if (result.Count == 0) {
                            Console.WriteLine("Socket closed, {0}: '{1}'", result.CloseStatus, result.CloseStatusDescription);
                            throw new WebSocketException("Socket closed while expecting data");
                        }
                        json = Encoding.UTF8.GetString(inbuffer, 0, result.Count);
                        Console.WriteLine(json);


                        bool needsubscribe = true;
                        while (true) {
                            if (needsubscribe) {
                                curid++;
                                var subscriptionpayload = new
                                {
                                    query = $"subscription {{ liveMeasurement(homeId: \"{config.HomeId}\") {{ timestamp power powerProduction accumulatedConsumption accumulatedProduction lastMeterConsumption lastMeterProduction powerFactor voltagePhase1 voltagePhase2 voltagePhase3 currentPhase1 currentPhase2 currentPhase3 }} }}"
                                };
                                message = JsonConvert.SerializeObject(new { type = "start", id = curid.ToString(), payload = subscriptionpayload });
                                Console.WriteLine(message);
                                buffer = Encoding.UTF8.GetBytes(message);
                                await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, timeout.Token);
                                needsubscribe = false;
                            }

                            // Timeout for receiving next message = 15s
                            timeout.CancelAfter(15000);
                            try {
                                result = await ws.ReceiveAsync(new ArraySegment<byte>(inbuffer), timeout.Token);
                            }
                            catch (OperationCanceledException) when (receiving && !cancelall.IsCancellationRequested) {
                                // Timeout after at least one message received - stop and restart
                                receiving = false;
                                needsubscribe = true;
                                timeout.Dispose();
                                timeout = CancellationTokenSource.CreateLinkedTokenSource(cancelall.Token);
                                timeout.CancelAfter(30000);
                                message = JsonConvert.SerializeObject(new { type = "stop", id = curid.ToString() });
                                Console.WriteLine(message);
                                buffer = Encoding.UTF8.GetBytes(message);
                                await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, cancelall.Token);
                                continue;
                            }
                            if (result.Count == 0) {
                                Console.WriteLine("Socket closed, {0}: '{1}'", result.CloseStatus, result.CloseStatusDescription);
                                throw new WebSocketException("Socket closed while expecting data");
                            }
                            json = Encoding.UTF8.GetString(inbuffer, 0, result.Count);
                            var data = JsonConvert.DeserializeObject<PulseData<LiveMeasurementData>>(json);
                            if (data.type == "complete" && data.id == curid.ToString()) {
                                receiving = false;
                                Console.WriteLine(json);
                                break;
                            }
                            var lm = data.payload.data.liveMeasurement;
                            receiving = true;
                            double? calcpower = (lm.voltagePhase1 * lm.currentPhase1 + lm.voltagePhase2 * lm.currentPhase2 + lm.voltagePhase3 * lm.currentPhase3) * lm.powerFactor;
                            double? calcpower2 = (lm.currentPhase1 + lm.currentPhase2 + lm.currentPhase3) * lm.powerFactor * 230;
                            Console.Write($"{lm.timestamp}: {lm.power} {lm.powerProduction}            \r");

                            DateTime date = DateTime.Parse(lm.timestamp);
                            Dictionary<string, object> values = new Dictionary<string, object>();
                            values.Add("powerConsumption", lm.power.Value);
                            if (lm.powerProduction.HasValue)
                                values.Add("powerProduction", lm.powerProduction.Value);
                            if (lm.accumulatedConsumption.HasValue)
                                values.Add("accumulatedConsumption", lm.accumulatedConsumption.Value);
                            if (lm.accumulatedProduction.HasValue)
                                values.Add("accumulatedProduction", lm.accumulatedProduction.Value);
                            if (lm.lastMeterConsumption.HasValue)
                                values.Add("lastMeterConsumption", lm.lastMeterConsumption.Value);
                            if (lm.lastMeterProduction.HasValue)
                                values.Add("lastMeterProduction", lm.lastMeterProduction.Value);
                            if (lm.powerFactor.HasValue)
                                values.Add("powerFactor", lm.powerFactor.Value);
                            if (lm.voltagePhase1.HasValue)
                                values.Add("voltagePhase1", lm.voltagePhase1.Value);
                            if (lm.voltagePhase2.HasValue)
                                values.Add("voltagePhase2", lm.voltagePhase2.Value);
                            if (lm.voltagePhase3.HasValue)
                                values.Add("voltagePhase3", lm.voltagePhase3.Value);
                            if (lm.currentPhase1.HasValue)
                                values.Add("currentPhase1", lm.currentPhase1.Value);
                            if (lm.currentPhase2.HasValue)
                                values.Add("currentPhase2", lm.currentPhase2.Value);
                            if (lm.currentPhase3.HasValue)
                                values.Add("currentPhase3", lm.currentPhase3.Value);
                            influx.Write(date, values);
                        }
                        message = JsonConvert.SerializeObject(new { type = "connection_terminate" });
                        Console.WriteLine(message);
                        buffer = Encoding.UTF8.GetBytes(message);
                        await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, timeout.Token);
                        await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Done", timeout.Token);
                    }
                    catch (WebSocketException wse) {
                        Console.WriteLine($"WebsocketException {wse.Message}");
                    }
                    catch (OperationCanceledException timeoutexception) when (!cancelall.IsCancellationRequested) {
                        Console.WriteLine($"Timeout: OperationCanceledException {timeoutexception.Message}");
                    }
        }
    }
}

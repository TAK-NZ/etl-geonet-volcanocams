import { Static, Type, TSchema } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType, InputFeatureCollection } from '@tak-ps/etl';

const Env = Type.Object({
    'Camera Proxy URL': Type.String({
        description: 'Base URL for camera proxy service',
        default: 'https://utils.test.tak.nz/camera-proxy/'
    })
});

// Define types for GeoNet Volcano Camera data structure
const CameraFeature = Type.Object({
    id: Type.String(),
    type: Type.Literal('Feature'),
    geometry: Type.Object({
        type: Type.Literal('Point'),
        coordinates: Type.Array(Type.Number(), { minItems: 2, maxItems: 2 })
    }),
    properties: Type.Object({
        title: Type.String(),
        height: Type.Number(),
        'latest-image-large': Type.String(),
        'latest-timestamp': Type.String(),
        azimuth: Type.Number()
    }),
    'volcano-id': Type.Array(Type.String()),
    'volcano-title': Type.Array(Type.String())
});

const VolcanoGroup = Type.Object({
    type: Type.Literal('FeatureCollection'),
    features: Type.Array(CameraFeature)
});

export default class Task extends ETL {
    static name = 'etl-geonet-volcanocams';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Env;
            } else {
                return CameraFeature;
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        try {
            const env = await this.env(Env);
            const cameraProxyUrl = env['Camera Proxy URL'].replace(/\/$/, '');
            
            console.log('ok - Fetching volcano camera data from GeoNet');
            
            const url = 'http://images.geonet.org.nz/volcano/cameras/all.json';
            const res = await fetch(url);
            
            if (!res.ok) {
                throw new Error(`Failed to fetch data: ${res.status} ${res.statusText}`);
            }
            
            const volcanoGroups = await res.json() as Static<typeof VolcanoGroup>[];
            const features: Static<typeof InputFeatureCollection>["features"] = [];
            
            for (const group of volcanoGroups) {
                for (const camera of group.features) {
                    const [lat, lon] = camera.geometry.coordinates;
                    const cameraUrl = `https://www.geonet.org.nz/volcano/cameras/${camera.id}`;
                    
                    const now = new Date().toISOString();
                    // Handle timezone-aware timestamp conversion
                    const timestampStr = camera.properties['latest-timestamp'];
                    // Parse the timestamp and handle NZ timezone manually
                    let cameraTime: string;
                    if (timestampStr.includes('NZST')) {
                        // NZST is UTC+12, so subtract 12 hours to get UTC
                        const localTime = new Date(timestampStr.replace(' NZST', ''));
                        const utcTime = new Date(localTime.getTime() - (12 * 60 * 60 * 1000));
                        cameraTime = utcTime.toISOString();
                    } else if (timestampStr.includes('NZDT')) {
                        // NZDT is UTC+13, so subtract 13 hours to get UTC
                        const localTime = new Date(timestampStr.replace(' NZDT', ''));
                        const utcTime = new Date(localTime.getTime() - (13 * 60 * 60 * 1000));
                        cameraTime = utcTime.toISOString();
                    } else {
                        cameraTime = new Date(timestampStr).toISOString();
                    }
                    features.push({
                        id: `volcano-camera-${camera.id}`,
                        type: 'Feature',
                        properties: {
                            callsign: camera.properties.title,
                            type: 'a-f-G-E-S',
                            how: 'm-g',
                            icon: 'ad78aafb-83a6-4c07-b2b9-a897a8b6a38f:Shapes/camera.png',
                            time: cameraTime,
                            start: cameraTime,
                            sensor: {
                                elevation: 0,
                                vfov: 45,
                                north: camera.properties.azimuth,
                                roll: 0,
                                range: 15000,
                                azimuth: camera.properties.azimuth,
                                fov: 45
                            },
                            'marker-color': 'rgb(25, 152, 123)',
                            links: [{
                                uid: `volcano-camera-${camera.id}-link`,
                                relation: 'r-u',
                                mime: 'text/html',
                                url: cameraUrl,
                                remarks: 'View Camera'
                            }],
                            remarks: [
                                `Camera: ${camera.properties.title}`,
                                `Volcano: ${camera['volcano-title'].join(', ')}`,
                                `Location: ${lat.toFixed(6)}, ${lon.toFixed(6)}`,
                                `Height: ${camera.properties.height}m`,
                                `Azimuth: ${camera.properties.azimuth}°`,
                                `Last Updated: ${camera.properties['latest-timestamp']}`
                            ].join('\n')
                        },
                        geometry: {
                            type: "Point",
                            coordinates: [lon, lat, camera.properties.height]
                        }
                    });
                }
            }
            
            const fc: Static<typeof InputFeatureCollection> = {
                type: 'FeatureCollection',
                features
            };
            console.log(`ok - fetched ${features.length} volcano cameras`);
            await this.submit(fc);
        } catch (error) {
            console.error(`Error in ETL process: ${error instanceof Error ? error.message : String(error)}`);
            throw error;
        }
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}


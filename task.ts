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



export default class Task extends ETL {
    static name = 'etl-geonet-volcanocams';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        return flow === DataFlowType.Incoming 
            ? (type === SchemaType.Input ? Env : CameraFeature)
            : Type.Object({});
    }

    private convertNZTimeToISO(timestampStr: string): string {
        const nzstOffset = 12 * 60 * 60 * 1000; // 12 hours in milliseconds
        const nzdtOffset = 13 * 60 * 60 * 1000; // 13 hours in milliseconds
        
        if (timestampStr.includes('NZST')) {
            const localTime = new Date(timestampStr.replace(' NZST', ''));
            return new Date(localTime.getTime() - nzstOffset).toISOString();
        } else if (timestampStr.includes('NZDT')) {
            const localTime = new Date(timestampStr.replace(' NZDT', ''));
            return new Date(localTime.getTime() - nzdtOffset).toISOString();
        }
        return new Date(timestampStr).toISOString();
    }

    async control() {
        try {
            console.log('ok - Fetching volcano camera data from GeoNet');
            
            const url = 'https://images.geonet.org.nz/volcano/cameras/all.json';
            const res = await fetch(url);
            
            if (!res.ok) {
                throw new Error(`Failed to fetch data: ${res.status} ${res.statusText}`);
            }
            
            const volcanoGroups = await res.json() as { type: string; features: Static<typeof CameraFeature>[] }[];
            const features: Static<typeof InputFeatureCollection>["features"] = [];
            
            for (const group of volcanoGroups) {
                for (const camera of group.features) {
                    const [lat, lon] = camera.geometry.coordinates;
                    const cameraUrl = `https://www.geonet.org.nz/volcano/cameras/${camera.id}`;
                    
                    // Optimized timezone conversion
                    const cameraTime = this.convertNZTimeToISO(camera.properties['latest-timestamp']);
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
            if (error instanceof TypeError) {
                console.error(`Network or parsing error: ${error.message}`);
                throw new Error(`Failed to fetch or parse volcano camera data: ${error.message}`);
            } else if (error instanceof Error) {
                console.error(`ETL processing error: ${error.message}`, { stack: error.stack });
                throw error;
            } else {
                console.error(`Unknown error in ETL process: ${String(error)}`);
                throw new Error(`Unexpected error occurred: ${String(error)}`);
            }
        }
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}


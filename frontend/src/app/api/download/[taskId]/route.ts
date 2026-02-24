import { NextRequest, NextResponse } from 'next/server';

export async function GET(
    request: NextRequest,
    context: { params: Promise<{ taskId: string }> }
) {
    const { taskId } = await context.params;

    try {
        // Proxy the download request to the FastAPI backend (same-server call, no CORS issues)
        const backendResponse = await fetch(`http://localhost:8000/api/download/${taskId}`);

        if (!backendResponse.ok) {
            const errorData = await backendResponse.json();
            return NextResponse.json(errorData, { status: backendResponse.status });
        }

        const jsonData = await backendResponse.json();
        const jsonString = JSON.stringify(jsonData, null, 2);

        const backendContentDisposition = backendResponse.headers.get('Content-Disposition') || 'attachment; filename="AutoGTM_Enhanced_Container.json"';

        // Return as a proper file download through the same-origin Next.js server
        return new NextResponse(jsonString, {
            status: 200,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                'Content-Disposition': backendContentDisposition,
                'Content-Length': Buffer.byteLength(jsonString, 'utf8').toString(),
            },
        });
    } catch (error) {
        return NextResponse.json(
            { error: 'Failed to fetch GTM container from backend' },
            { status: 500 }
        );
    }
}

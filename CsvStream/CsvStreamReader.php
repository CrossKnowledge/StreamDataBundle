<?php
namespace CrossKnowledge\StreamDataBundle\CsvStream;

class CsvStreamReader
{
    /**
     * 2097152 bytes = 2 Mb
     */
    const FREAD_LENGTH = 2097152;

    /**
     * Specific expected header
     */
    const HEADER_CONTENT_COUNT = 'Content-Count';

    /**
     * Unactivity max delay before closing the connection
     */
    const UNACTIVITY_DELAY = 60;

    private $uri = '';

    private $client = null;

    private $callback;

    private $maxDelay;

    private $contentCountTotal = 0;

    private $contentCount = 0;

    private $reliquat = '';

    /**
     */
    public function __construct($uri, $callback = null, $maxDelay = self::UNACTIVITY_DELAY)
    {
        $this->uri = $uri;
        $this->callback = $callback;
        $this->maxDelay = $maxDelay;
        $this->client = new \GuzzleHttp\Client();
    }

    public function getResponse()
    {
        $response = $this->client->request('GET', $this->uri,
            [
                'stream' => true,
                'sink' => fopen('php://stdout', 'w')
            ]);
        $stream = $response->getBody();
        $this->contentCountTotal = $response->getHeader(self::HEADER_CONTENT_COUNT)[0];
        $cptLoop = 1;
        while (!$stream->eof()) {
            $lines = explode(PHP_EOL, $this->reliquat . $stream->read(self::FREAD_LENGTH));
            $nbLines = count($lines);
            if (substr($lines[$nbLines-1], -1, 1) == PHP_EOL) {
                $this->reliquat = '';
            } else {
                $this->reliquat = $lines[$nbLines-1];
                unset($lines[$nbLines-1]);
            }

            call_user_func($this->callback, $lines, $cptLoop);

            $this->contentCount += count($lines);

            if ($this->contentCount >= $this->contentCountTotal) {
                $stream->close();
                break;
            }
            $cptLoop ++;
        }
    }
}
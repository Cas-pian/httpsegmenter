/* -*- Mode: c; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*-
 NOTE: the first line of this file sets up source code indentation rules
 for Emacs; it is also a hint to anyone modifying this file.
 */
/* $Id: segmenter.c,v 1.6 2014/03/17 06:03:00 wanggm Exp $
 * $HeadURL
 *
 * Copyright (c) 2009 Chase Douglas
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License version 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
 
#ifdef _WIN32
	#include <windows.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <float.h>
#include <math.h>

#include "libavformat/avformat.h"
#include "libavutil/opt.h"

// add by houmr
#include "libavutil/timestamp.h"

//add by wanggm
#include <json/json.h>
#include <json/json_object.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>

const int INNERDIFFERENCE = 180000;
const int OUTERDIFFERENCE = 180000;

char timeChar[21] = {'\0'};

#ifdef _WIN32
//----------------------------------------------------------------
// utf8_to_utf16
// 
static wchar_t *
utf8_to_utf16(const char * str_utf8)
{
	int nchars = MultiByteToWideChar(CP_UTF8, 0, str_utf8, -1, NULL, 0);
	wchar_t * str_utf16 = (wchar_t *)malloc(nchars * sizeof(wchar_t));
	MultiByteToWideChar(CP_UTF8, 0, str_utf8, -1, str_utf16, nchars);
	return str_utf16;
}

//----------------------------------------------------------------
// utf16_to_utf8
// 
static char *
utf16_to_utf8(const wchar_t * str_utf16)
{
	int nchars = WideCharToMultiByte(CP_UTF8, 0, str_utf16, -1, NULL, 0, 0, 0);
	char * str_utf8 = (char *)malloc(nchars * sizeof(char));
	WideCharToMultiByte(CP_UTF8, 0, str_utf16, -1, str_utf8, nchars, 0, 0);
	return str_utf8;
}
#endif

//----------------------------------------------------------------
// fopen_utf8
// 
static FILE *
fopen_utf8(const char * filename, const char * mode)
{
	FILE * file = NULL;

#ifdef _WIN32
	wchar_t * wname = utf8_to_utf16(filename);
	wchar_t * wmode = utf8_to_utf16(mode);
	file = _wfopen(wname, wmode);
	free(wname);
	free(wmode);
#else
	file = fopen(filename, mode);
#endif

	return file;
}

//add by wanggm for logging the time
char * getSystemTime(char *dst)
{
	struct tm *nowInSeconds;
	time_t lt; 
	lt = time(NULL);
	nowInSeconds = localtime(&lt);

	strftime(dst, 20, "%Y-%m-%d %H:%M:%S", nowInSeconds);
	return dst;
}

//add by wanggm for mointor the process of the given pid, exit this process when the process exit.
int monitor_process(pid_t pid)
{
	fprintf(stdout, "begin to monitor the process PID = %ld\n", (long)pid);
	if(pid <= 0)
	{
		fprintf(stderr, "process pid <0 is invalid!\n");
		exit(-1);
	}
	while(1)
	{
			int ret = kill(pid, 0);
			if( -1 == ret && ESRCH == errno)
			{ 
				fprintf(stdout, "\n%s The process PID=%ld has already exit, sorry to byebye...\n", getSystemTime(timeChar), (long)pid);
				fflush(stdout);
				break;
			}
			//fprintf(stdout, "process pid=%ld is still running.\n", (long)pid);
			sleep(1);
	}
	exit(1);
}

static AVStream *add_output_stream(AVFormatContext *output_format_context,
		AVStream *input_stream)
{
	AVCodecContext *input_codec_context;
	AVCodecContext *output_codec_context;
	AVStream *output_stream;

	output_stream = avformat_new_stream(output_format_context, NULL);
	if (!output_stream)
	{
		fprintf(stderr, "%s Could not allocate stream\n", getSystemTime(timeChar));
		exit(1);
	}
	output_stream->id = 0;

	input_codec_context = input_stream->codec;
	output_codec_context = output_stream->codec;

	output_codec_context->codec_id = input_codec_context->codec_id;
	output_codec_context->codec_type = input_codec_context->codec_type;
	output_codec_context->codec_tag = input_codec_context->codec_tag;
	output_codec_context->bit_rate = input_codec_context->bit_rate;
	output_codec_context->extradata = input_codec_context->extradata;
	output_codec_context->extradata_size = input_codec_context->extradata_size;

	if (av_q2d(input_codec_context->time_base)
			* input_codec_context->ticks_per_frame
			> av_q2d(input_stream->time_base)
			&& av_q2d(input_stream->time_base) < 1.0 / 1000)
	{
		output_codec_context->time_base = input_codec_context->time_base;
		output_codec_context->time_base.num *=
				input_codec_context->ticks_per_frame;
	}
	else
	{
		output_codec_context->time_base = input_stream->time_base;
	}

	switch (input_codec_context->codec_type)
	{
	case AVMEDIA_TYPE_AUDIO:
		output_codec_context->channel_layout =
				input_codec_context->channel_layout;
		output_codec_context->sample_rate = input_codec_context->sample_rate;
		output_codec_context->channels = input_codec_context->channels;
		output_codec_context->frame_size = input_codec_context->frame_size;
		if ((input_codec_context->block_align == 1
				&& input_codec_context->codec_id == CODEC_ID_MP3)
				|| input_codec_context->codec_id == CODEC_ID_AC3)
		{
			output_codec_context->block_align = 0;
		}
		else
		{
			output_codec_context->block_align =
					input_codec_context->block_align;
		}
		break;
	case AVMEDIA_TYPE_VIDEO:
		output_codec_context->pix_fmt = input_codec_context->pix_fmt;
		output_codec_context->width = input_codec_context->width;
		output_codec_context->height = input_codec_context->height;
		output_codec_context->has_b_frames = input_codec_context->has_b_frames;

		if (output_format_context->oformat->flags & AVFMT_GLOBALHEADER)
		{
			output_codec_context->flags |= CODEC_FLAG_GLOBAL_HEADER;
		}
		break;
	default:
		break;
	}

	return output_stream;
}

typedef struct SMSegmentInfo
{
	unsigned int index;
	double duration;
	char * filename;

} TSMSegmentInfo;

typedef struct SMPlaylist
{
	/* a ring buffer of segments */
	TSMSegmentInfo * buffer;

	/* maximum number of segments that can be stored in the ring buffer */
	unsigned int bufferCapacity;

	/* index of the first segment on the ring buffer */
	unsigned int first;

	/* how many segments are currently in the ring buffer */
	unsigned int count;

	/* shortcuts */
	unsigned int targetDuration;
	char * httpPrefix;

	/* playlist file used for non-live streaming */
	FILE * file;

} TSMPlaylist;

static char *
duplicateString(const char * str)
{
	/* unfortunately strdup isn't always available */
	size_t strSize = strlen(str) + 1;
	char * copy = (char *) malloc(strSize);
	memcpy(copy, str, strSize);
	return copy;
}

static TSMPlaylist *
createPlaylist(const unsigned int max_segments,
		const unsigned int target_segment_duration, const char * http_prefix)
{
	TSMPlaylist * playlist = (TSMPlaylist *) malloc(sizeof(TSMPlaylist));
	memset(playlist, 0, sizeof(TSMPlaylist));

	if (max_segments)
	{
		playlist->buffer = (TSMSegmentInfo *) malloc(
				sizeof(TSMSegmentInfo) * max_segments);
	}

	playlist->bufferCapacity = max_segments;
	playlist->targetDuration = target_segment_duration;
	playlist->httpPrefix = duplicateString(http_prefix);

	return playlist;
}

static void updateLivePlaylist(TSMPlaylist * playlist,
		const char * playlistFileName, const char * outputFileName,
		const unsigned int segmentIndex, const double segmentDuration)
{
	unsigned int bufferIndex = 0;
	TSMSegmentInfo * nextSegment = NULL;
	TSMSegmentInfo removeMe;
	memset(&removeMe, 0, sizeof(removeMe));
	assert(!playlist->file);

	if (playlist->count == playlist->bufferCapacity)
	{
		/* keep track of the segment that should be removed */
		removeMe = playlist->buffer[playlist->first];

		/* make room for the new segment */
		playlist->first++;
		playlist->first %= playlist->bufferCapacity;
	}
	else
	{
		playlist->count++;
	}

	/* store the new segment info */
	bufferIndex = ((playlist->first + playlist->count - 1)
			% playlist->bufferCapacity);
	nextSegment = &playlist->buffer[bufferIndex];
	nextSegment->filename = duplicateString(outputFileName);
	nextSegment->duration = segmentDuration;
	nextSegment->index = segmentIndex;

	/* live streaming -- write full playlist from scratch */
	playlist->file = fopen_utf8(playlistFileName, "w+b");

	if (playlist->file)
	{
		const TSMSegmentInfo * first = &playlist->buffer[playlist->first];

		char tmp[1024] =
		{ 0 };
		snprintf(tmp, sizeof(tmp), "#EXTM3U\n"
				"#EXT-X-TARGETDURATION:%u\n"
				"#EXT-X-MEDIA-SEQUENCE:%u\n", playlist->targetDuration,
				first->index);
		fwrite(tmp, strlen(tmp), 1, playlist->file);
		unsigned int i;
		for (i = 0; i < playlist->count; i++)
		{
			unsigned int j = ((playlist->first + i) % playlist->bufferCapacity);

			const TSMSegmentInfo * segment = &playlist->buffer[j];
			snprintf(tmp, sizeof(tmp), "#EXTINF:%u,\n%s%s\n",
					(int) (segment->duration + 0.5), playlist->httpPrefix,
					segment->filename);
			fwrite(tmp, strlen(tmp), 1, playlist->file);
		}

		// snprintf(tmp, sizeof(tmp), "#EXT-X-ENDLIST\n");
		// fwrite(tmp, strlen(tmp), 1, playlist->file);

		fclose(playlist->file);
		playlist->file = NULL;
	}
	else
	{
		fprintf(stderr, "%s Could not open m3u8 index file (%s), "
				"no index file will be created\n", getSystemTime(timeChar)
				,playlistFileName);
	}

	if (removeMe.filename)
	{
		/* remove the oldest segment file */
		remove(removeMe.filename);

		//add by wanggm begin.
		char indexfile[128]={'\0'};
		char *index = indexfile;
		memcpy(indexfile, removeMe.filename, strlen(removeMe.filename) - 2);
		strcat(index, "idx");
		remove(indexfile);
		//add by wanggm end.

		free(removeMe.filename);
	}
}

static void updatePlaylist(TSMPlaylist * playlist,
		const char * playlistFileName, const char * segmentFileName,
		const unsigned int segmentIndex, const int segmentDuration)
{
	if (playlist->bufferCapacity > 0)
	{
		/* create a live streaming playlist */
		updateLivePlaylist(playlist, playlistFileName, segmentFileName,
				segmentIndex, segmentDuration);
	}
	else
	{
		/* append to the existing playlist */
		char tmp[1024] =
		{ 0 };

		if (!playlist->file)
		{
			playlist->file = fopen_utf8(playlistFileName, "w+b");
			snprintf(tmp, sizeof(tmp), "#EXTM3U\n"
					"#EXT-X-TARGETDURATION:%u\n", playlist->targetDuration);
			fwrite(tmp, strlen(tmp), 1, playlist->file);
		}

		if (!playlist->file)
		{
			fprintf(stderr, "%s Could not open m3u8 index file (%s), "
					"no index file will be created\n", getSystemTime(timeChar)
					,playlistFileName);
		}

		snprintf(tmp, sizeof(tmp), "#EXTINF:%u,\n%s%s\n", segmentDuration,
				playlist->httpPrefix, segmentFileName);
		fwrite(tmp, strlen(tmp), 1, playlist->file);
		fflush(playlist->file);
	}
}

static void closePlaylist(TSMPlaylist * playlist)
{
	if (playlist->file)
	{
		/* append to the existing playlist */
		char tmp[1024] =
		{ 0 };

		snprintf(tmp, sizeof(tmp), "#EXT-X-ENDLIST\n");
		fwrite(tmp, strlen(tmp), 1, playlist->file);

		fclose(playlist->file);
		playlist->file = NULL;
	}
}

static void releasePlaylist(TSMPlaylist ** playlistRef)
{
	TSMPlaylist * playlist = *playlistRef;
	closePlaylist(playlist);

	unsigned int i;
	for (i = 0; i < playlist->bufferCapacity; i++)
	{
		TSMSegmentInfo * segmentInfo = &playlist->buffer[i];
		if (segmentInfo->filename && strlen(segmentInfo->filename) > 0)
		{
			free(segmentInfo->filename);
		}
	}

	free(playlist->buffer);
	free(playlist->httpPrefix);
	free(playlist);
	*playlistRef = NULL;
}

typedef struct SMPacketLink
{
	/* packet start time in seconds */
	double timeStamp;

	/* the packet */
	AVPacket packet;

	/* a link to the next packet */
	struct SMPacketLink * next;

} TSMPacketLink;

typedef struct SMPacketList
{
	TSMPacketLink * head;
	TSMPacketLink * tail;
	unsigned int size;
} TSMPacketList;

typedef struct SMStreamLace
{
	TSMPacketList ** streams;
	unsigned int numStreams;
} TSMStreamLace;

static TSMPacketLink *
createLink(const AVPacket * packet, double timeStamp)
{
	TSMPacketLink * link = (TSMPacketLink *) malloc(sizeof(TSMPacketLink));
	link->timeStamp = timeStamp;
	link->next = NULL;
	memcpy(&link->packet, packet, sizeof(AVPacket));
	return link;
}

static void fifoPush(TSMPacketList * packets, const AVPacket * packet,
		double timeStamp)
{
	TSMPacketLink * link = createLink(packet, timeStamp);
	if (!packets->head)
	{
		assert(!packets->tail);
		assert(!packets->size);
		packets->head = link;
		packets->tail = link;
		packets->size = 1;
	}
	else
	{
		/* attach at the tail */
		assert(packets->size > 0);

		packets->tail->next = link;
		packets->tail = link;
		packets->size++;
	}
}

static int fifoPop(TSMPacketList * packets, AVPacket * packet)
{
	TSMPacketLink * link = packets->head;
	if (!link)
	{
		return 0;
	}

	memcpy(packet, &link->packet, sizeof(AVPacket));
	packets->head = link->next;
	packets->size--;

	if (!packets->head)
	{
		packets->tail = NULL;
	}

	free(link);
	return 1;
}

static TSMPacketList *
createPacketList()
{
	TSMPacketList * packets = (TSMPacketList *) malloc(sizeof(TSMPacketList));
	memset(packets, 0, sizeof(TSMPacketList));
	return packets;
}

static TSMStreamLace *
createStreamLace(unsigned int numStreams)
{
	TSMStreamLace * lace = (TSMStreamLace *) malloc(sizeof(TSMStreamLace));
	lace->streams = (TSMPacketList **) malloc(
			sizeof(TSMPacketList *) * numStreams);
	unsigned int i;
	for (i = 0; i < numStreams; i++)
	{
		lace->streams[i] = createPacketList();
	}

	lace->numStreams = numStreams;
	return lace;
}

static void insertPacket(TSMStreamLace * lace, const AVPacket * packet,
		double timeStamp)
{
	fifoPush(lace->streams[packet->stream_index], packet, timeStamp);
}

static TSMPacketList *
chooseNextStream(TSMStreamLace * lace)
{
	/* improve lacing so that that audio/video packets that should be
	 together do not get stuck into separate segments. */

	TSMPacketList * nextStream = NULL;
	double earliestTimeStamp = DBL_MAX;
	unsigned int i;
	for (i = 0; i < lace->numStreams; i++)
	{
		TSMPacketList * stream = lace->streams[i];
		if (stream->size && stream->head->timeStamp < earliestTimeStamp)
		{
			nextStream = stream;
			earliestTimeStamp = stream->head->timeStamp;
		}
	}

	return nextStream;
}

static int removePacket(TSMStreamLace * lace, AVPacket * packet)
{
	TSMPacketList * stream = chooseNextStream(lace);
	if (!stream)
	{
		return 0;
	}

	return fifoPop(stream, packet);
}

static unsigned int countPackets(const TSMStreamLace * lace)
{
	unsigned int numPackets = 0;
	unsigned int i;
	for (i = 0; i < lace->numStreams; i++)
	{
		const TSMPacketList * stream = lace->streams[i];
		numPackets += stream->size;
	}

	return numPackets;
}

static void removeAllPackets(TSMStreamLace * lace)
{
	AVPacket packet;
	unsigned int i;
	for (i = 0; i < lace->numStreams; i++)
	{
		TSMPacketList * stream = lace->streams[i];
		while (stream->size)
		{
			fifoPop(stream, &packet);
			av_free_packet(&packet);
		}
	}
}

static int loglevel(const char* arg)
{
	const struct
	{
		const char *name;
		int level;
	} log_levels[] =
	{
	{ "quiet", AV_LOG_QUIET },
	{ "panic", AV_LOG_PANIC },
	{ "fatal", AV_LOG_FATAL },
	{ "error", AV_LOG_ERROR },
	{ "warning", AV_LOG_WARNING },
	{ "info", AV_LOG_INFO },
	{ "verbose", AV_LOG_VERBOSE },
	{ "debug", AV_LOG_DEBUG }, };
	int i;

	for (i = 0; i < FF_ARRAY_ELEMS(log_levels); i++)
	{
		if (!strcmp(log_levels[i].name, arg))
		{
			av_log_set_level(log_levels[i].level);
			return 0;
		}
	}

	return 1;
}

//----------------------------------------------------------------
// usage3
// 
static void usage3(char ** argv, const char * message, const char * details)
{
	if (message)
	{
		fprintf(stderr, "ERROR: %s%s\n", message, details);
	}

	fprintf(stderr, "USAGE: %s \n"
			"\t-i input-MPEG-TS-file\n"
			"\t-d seconds-per-segment\n"
			"\t[-o segment-file-prefix]\n"
			"\t-x output-playlist-m3u8 \n"
			"\t[-p http-prefix] \n"
			"\t[-w max-live-segments] \n"
			"\t[-P pid-file] \n"
			"\t[-k if-create-idx-file]\n"
			"\t[-m monitor-related-process-pid]\n"
			"\t[-s start-index-of-ts-file]\n"
			"\t[--watch-for-kill-file] \n"
			"\t[--strict-segment-duration] \n"
			"\t[--avformat-option opt value] \n"
			"\t[--loglevel level] \n"
			"\n", argv[0]);

	fprintf(stderr,
			"Compiled by Daniel Espendiller - www.espend.de\n"
					"build on %s %s with %s\n\n"
					"Took some code from:\n"
					" - source:http://svn.assembla.com/svn/legend/segmenter/\n"
					" - iStreamdev:http://projects.vdr-developer.org/git/?p=istreamdev.git;a=tree;f=segmenter;hb=HEAD\n"
					" - live_segmenter:http://github.com/carsonmcdonald/HTTP-Live-Video-Stream-Segmenter-and-Distributor\n",
			__DATE__, __TIME__, __VERSION__);

	exit(1);
}

//----------------------------------------------------------------
// usage
// 
static void usage(char ** argv, const char * message)
{
	usage3(argv, message, "");
}

// add by houmr
int adjust_pts(AVPacket *packet, int video_index, int audio_index)
{
	static int64_t pts_offset = 0;
	static int64_t last_video_pts = 0;
	static int64_t last_audio_pts = 0;
	static int last_video_duration = 0;
	static int last_audio_duration = 0;
	int64_t *index_last_pts;
	int64_t *index_other_last_pts;
	int *index_last_duration;
	if (packet->stream_index == video_index)
	{
		index_last_pts = &last_video_pts;
		index_other_last_pts = &last_audio_pts;
		index_last_duration = &last_video_duration;
	}
	else if (packet->stream_index == audio_index)
	{
		index_last_pts = &last_audio_pts;
		index_other_last_pts = &last_video_pts;
		index_last_duration = &last_audio_duration;
	}
	else
	{
		return 0;
	}
	// first video packet
	if (*index_last_pts == 0)
	{
		*index_last_pts = packet->pts;
		*index_last_duration = packet->duration;
	}
	else
	{
		if (packet->pts == AV_NOPTS_VALUE)
		{
			// Ã»ÓÐpts,
			printf("pts = NOPTS\n");
			return 0;
		}

		int64_t new_pts = packet->pts + pts_offset;
		//printf("llabs(new_pts - last_pts) = %s\n",
		// av_ts2str(llabs(new_pts - *index_last_pts)));
		printf("old_pts=%ld\t", (long)packet->pts);
		if (new_pts < *index_last_pts || ( (new_pts - *index_last_pts > INNERDIFFERENCE ) && (new_pts - *index_other_last_pts > OUTERDIFFERENCE )))
		{
			pts_offset = *index_last_pts - packet->pts + *index_last_duration;
			new_pts = packet->pts + pts_offset;
			printf("index - %d\tguess a new clip! New offset= %ld\n",packet->stream_index, (long) pts_offset);
		}

		printf("new_pts=%ld\n", (long)new_pts);
		packet->pts = new_pts;
		packet->dts = new_pts;
		*index_last_pts = packet->pts;
	
		if (packet->duration != 0)
			*index_last_duration = packet->duration;
	}

	//printf("dst : index - %d\t pts = %s\t duration=%d\t last pts=%s\n", packet->stream_index,
	 //av_ts2str(packet->pts), packet->duration, av_ts2str(*index_last_pts));
	return 0;
}
//////////////////////////

//add by wanggm begin
struct json_object* create_json_header(struct json_object *obj)
{ 
	static struct json_object *arrObj;
	
	arrObj = json_object_new_array();
	json_object_object_add(obj, "index", arrObj);
	//fprintf(stdout, "%s create_json_header()\n", getSystemTime( timeChar));
	return arrObj;
}

int save_keyframe_info(struct json_object *arr_obj, long p, long t)
{
	if(NULL == arr_obj)
	{
		return -1;
	}
	//fprintf(stdout, "%s insert 1 keyframe into json obj.", getSystemTime(timeChar));
	struct json_object *infoObj = json_object_new_object();
	json_object_object_add(infoObj, "p", json_object_new_int64(p));
	json_object_object_add(infoObj, "t", json_object_new_int64(t));
	
	json_object_array_add(arr_obj, infoObj);

	return 0;
}

int save_json_to_file(const char* file_name, struct json_object *obj)
{  
	int len = strlen(json_object_get_string(obj));
	char arr[len+1] ;
	memcpy(arr, json_object_get_string(obj), len);
	
	//fprintf(stdout,"%s Save json to file. filename=%s jsonContent=%s\n", getSystemTime( timeChar), file_name, arr);

	FILE *fp = fopen(file_name, "w");
	if (NULL == fp)
	{ 
		fprintf(stderr, "%s Open file failed ! filename=%s\n", getSystemTime( timeChar),file_name);
	}else
	{ 
		int ret = fwrite(arr, len, 1, fp);
		if ( ret < 1)
		{ 
			fprintf(stderr, "%s write to file failed ! file_name=%s\n",getSystemTime( timeChar), file_name);
		}

		fclose(fp);
	}

	json_object_put(obj); 	//free the space.

	return 0;
}
//add by wanggm end


//----------------------------------------------------------------
// main_utf8
// 
int main_utf8(int argc, char **argv)
{
	const char *input = NULL;
	const char *output_prefix = "";
	double target_segment_duration = 0.0;
	char *segment_duration_check = NULL;
	const char *playlist_filename = NULL;
	const char *http_prefix = "";
	long max_tsfiles = 0;
	char *max_tsfiles_check = NULL;
	double prev_segment_time = 0.0;
	double segment_duration = 0.0;
	unsigned int output_index = 1;
	const AVClass *fc = avformat_get_class();
	AVDictionary *format_opts = NULL;
	AVOutputFormat *ofmt = NULL;
	AVFormatContext *ic = NULL;
	AVFormatContext *oc = NULL;
	AVStream *video_st = NULL;
	AVStream *audio_st = NULL;
	AVCodec *codec = NULL;
	char *output_filename = NULL; 	
	int if_save_keyframe = 0;			//add by wanggm
	char *keyframeinfo_filename = NULL;	//add by wanggm
	json_object *obj = NULL;			//add by wanggm
	json_object *info_arr_obj = NULL;	//add by wanggm

	int if_monitor_related_process = 0;	//add by wanggm
	pid_t relatedProcessPid = 1; 		//add by wanggm
	char *pid_filename = NULL;
	int video_index = -1;
	int audio_index = -1;
	int kill_file = 0;
	int decode_done = 0;
	int ret = 0;
	int i = 0;
	TSMStreamLace * streamLace = NULL;
	TSMPlaylist * playlist = NULL;
	const double segment_duration_error_tolerance = 0.05;
	double extra_duration_needed = 0;
	int strict_segment_duration = 0;

	av_log_set_level(AV_LOG_INFO);


	for (i = 1; i < argc; i++)
	{
		if (strcmp(argv[i], "-i") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -i parameter");
			i++;
			input = argv[i];
		}
		else if (strcmp(argv[i], "-o") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -o parameter");
			i++;
			output_prefix = argv[i];
		}
		else if (strcmp(argv[i], "-d") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -d parameter");
			i++;

			target_segment_duration = strtod(argv[i], &segment_duration_check);
			if (segment_duration_check
					== argv[i] || target_segment_duration == HUGE_VAL
					|| target_segment_duration == -HUGE_VAL){
				usage3(argv, "invalid segment duration: ", argv[i]);
			}
		}
		else if (strcmp(argv[i], "-x") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -x parameter");
			i++;
			playlist_filename = argv[i];
		}
		else if (strcmp(argv[i], "-p") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -p parameter");
			i++;
			http_prefix = argv[i];
		}
		else if (strcmp(argv[i], "-w") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -w parameter");
			i++;

			max_tsfiles = strtol(argv[i], &max_tsfiles_check, 10);
			if (max_tsfiles_check == argv[i] || max_tsfiles < 0
					|| max_tsfiles >= INT_MAX)
			{
				usage3(argv, "invalid live stream max window size: ", argv[i]);
			}
		}
		else if (strcmp(argv[i], "-P") == 0)
		{
			if ((argc - i) <= 1)
				usage(argv, "could not parse -P parameter");
			i++;
			pid_filename = argv[i];
		}
		else if (strcmp(argv[i], "--watch-for-kill-file") == 0)
		{
			// end program when it finds a file with name 'kill':
			kill_file = 1;
		}
		else if (strcmp(argv[i], "--strict-segment-duration") == 0)
		{
			// force segment creation on non-keyframe boundaries:
			strict_segment_duration = 1;
		}
		else if (strcmp(argv[i], "--avformat-option") == 0)
		{
			const AVOption *of;
			const char *opt;
			const char *arg;
			if ((argc - i) <= 1)
				usage(argv, "could not parse --avformat-option parameter");
			i++;
			opt = argv[i];
			if ((argc - i) <= 1)
				usage(argv, "could not parse --avformat-option parameter");
			i++;
			arg = argv[i];

			if ((of = av_opt_find(&fc, opt, NULL, 0,
					AV_OPT_SEARCH_CHILDREN | AV_OPT_SEARCH_FAKE_OBJ)))
				av_dict_set(&format_opts, opt, arg,
						(of->type == AV_OPT_TYPE_FLAGS) ? AV_DICT_APPEND : 0);
			else
				usage3(argv, "unknown --avformat-option parameter: ", opt);
		}
		else if (strcmp(argv[i], "--loglevel") == 0)
		{
			const char *arg;
			if ((argc - i) <= 1)
				usage(argv, "could not parse --loglevel parameter");
			i++;
			arg = argv[i];

			if (loglevel(arg))
				usage3(argv, "unknown --loglevel parameter: ", arg);
		}
		else if (strcmp(argv[i], "-k") == 0)
		{ //add by wanggm for save key frame information into json file.
			if ((argc - i) <= 1)
				usage(argv, "could not parse -k parameter");
			i++;
			if_save_keyframe = atoi(argv[i]);
		}
		else if( strcmp(argv[i], "-s") == 0)
		{//add by wanggm for set the start index of ts file.
			if ( (argc -i ) <= 1)
				usage(argv, "could not parse -s parmeter");
			i++;
			
			char *output_index_check = NULL;
			output_index  = strtol(argv[i], &output_index_check, 10);
			if ( output_index_check== argv[i] || output_index < 0
					|| output_index >= INT_MAX)
			{
				usage3(argv, "invalid start index of ts file: ", argv[i]);
			}
			
		}
		else if( strcmp(argv[i], "-m") == 0)
		{ // add by wanggm for exit by monitor the process of which pid is given. 
			if ((argc - i) <= 1)
				usage(argv, "could not parse -m parmeter");
			i++;

			if_monitor_related_process = 1;
			unsigned int tmpPid= atoi(argv[i]);
			if( tmpPid > 0)
			{  
				relatedProcessPid = (pid_t) tmpPid;
				fprintf(stdout, "%s I will exit when the process PID= %d exit.\n", getSystemTime(timeChar), relatedProcessPid);
			}
		}
	}

	
	if (!input)
	{
		usage(argv, "-i input file parameter must be specified");
	}

	if (!playlist_filename)
	{
		usage(argv, "-x m3u8 playlist file parameter must be specified");
	}

	if (target_segment_duration == 0.0)
	{
		usage(argv, "-d segment duration parameter must be specified");
	}

	if( output_index <= 0 )
	{
		output_index = 1;	
	}

	if( 1 == if_monitor_related_process)
	{
		pthread_t id;
		pthread_create(&id, NULL, (void*)monitor_process, relatedProcessPid);
	}


	// Create PID file
	if (pid_filename)
	{
		FILE* pid_file = fopen_utf8(pid_filename, "wb");
		if (pid_file)
		{
			fprintf(pid_file, "%d", getpid());
			fclose(pid_file);
		}
	}


	av_register_all();
	avformat_network_init();

	if (!strcmp(input, "-"))
	{
		input = "pipe:";
	}

	output_filename = (char*) malloc(
			sizeof(char) * (strlen(output_prefix) + 15));
	//add by wanggm
	if(  if_save_keyframe == 1)
	{ 
		keyframeinfo_filename = (char*) malloc(
			sizeof(char)* (strlen(output_prefix) + 15));
	}
	if (!output_filename || (1 == if_save_keyframe && !keyframeinfo_filename))
	{
		fprintf(stderr, "%s Could not allocate space for output filenames\n", getSystemTime( timeChar));
		goto error;
	}

	playlist = createPlaylist(max_tsfiles, target_segment_duration,
			http_prefix);
	if (!playlist)
	{
		fprintf(stderr,
				"%s Could not allocate space for m3u8 playlist structure\n", getSystemTime( timeChar));
		goto error;
	}

	ret = avformat_open_input(&ic, input, NULL, (format_opts) ? &format_opts : NULL);

	if (ret != 0)
	{
		fprintf(stderr,
				"%sCould not open input file, make sure it is an mpegts or mp4 file: %d\n", getSystemTime(timeChar), ret);
		goto error;
	}
	av_dict_free(&format_opts);

	if (avformat_find_stream_info(ic, NULL) < 0)
	{
		fprintf(stderr, "%s Could not read stream information\n", getSystemTime( timeChar));
		goto error;
	}

#if LIBAVFORMAT_VERSION_MAJOR > 52 || (LIBAVFORMAT_VERSION_MAJOR == 52 && \
                                       LIBAVFORMAT_VERSION_MINOR >= 45)
	ofmt = av_guess_format("mpegts", NULL, NULL);
#else
	ofmt = guess_format("mpegts", NULL, NULL);
#endif

	if (!ofmt)
	{
		fprintf(stderr, "%s Could not find MPEG-TS muxer\n", getSystemTime( timeChar));
		goto error;
	}

	oc = avformat_alloc_context();
	if (!oc)
	{
		fprintf(stderr, "%s Could not allocated output context\n", getSystemTime( timeChar));
		goto error;
	}
	oc->oformat = ofmt;

	video_index = -1;
	audio_index = -1;

	for (i = 0; i < ic->nb_streams && (video_index < 0 || audio_index < 0); i++)
	{
		switch (ic->streams[i]->codec->codec_type)
		{
		case AVMEDIA_TYPE_VIDEO:
			video_index = i;
			ic->streams[i]->discard = AVDISCARD_NONE;
			video_st = add_output_stream(oc, ic->streams[i]);
			break;
		case AVMEDIA_TYPE_AUDIO:
			audio_index = i;
			ic->streams[i]->discard = AVDISCARD_NONE;
			audio_st = add_output_stream(oc, ic->streams[i]);
			break;
		default:
			ic->streams[i]->discard = AVDISCARD_ALL;
			break;
		}
	}

	av_dump_format(oc, 0, output_prefix, 1);

	if (video_index >= 0)
	{
		codec = avcodec_find_decoder(video_st->codec->codec_id);
		if (!codec)
		{
			fprintf(stderr,
					"%s Could not find video decoder, key frames will not be honored\n", getSystemTime( timeChar));
		}

		if (avcodec_open2(video_st->codec, codec, NULL) < 0)
		{
			fprintf(stderr,
					"%s Could not open video decoder, key frames will not be honored\n", getSystemTime( timeChar));
		}
	}

	snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts",
			output_prefix, output_index);

	if( 1 == if_save_keyframe)
	{ 
		snprintf(keyframeinfo_filename, strlen(output_prefix) + 15,
			"%s-%u.idx", output_prefix, output_index);
		obj = json_object_new_object();
		info_arr_obj = create_json_header(obj);
	}

	if (avio_open(&oc->pb, output_filename, AVIO_FLAG_WRITE) < 0)
	{
		fprintf(stderr, "%s Could not open '%s'\n", getSystemTime( timeChar),output_filename);
		goto error;
	}

	if (avformat_write_header(oc, NULL))
	{
		fprintf(stderr, "%s Could not write mpegts header to first output file\n", getSystemTime( timeChar));
		goto error;
	}

	prev_segment_time = (double) (ic->start_time) / (double) (AV_TIME_BASE);

	streamLace = createStreamLace(ic->nb_streams);

	// add by houmr
	int continue_error_cnt = 0;
	//int pushcnt = 0;
	//int popcnt = 0;
	int tscnt = 0;
	int audiopktcnt = 0;
	int videopktcnt = 0;
	int kfcnt = 0;
	int errpktcnt = 0;
	/////////////////////////
	do
	{
		double segment_time = 0.0;
		AVPacket packet;
		double packetStartTime = 0.0;
		double packetDuration = 0.0;

		if (!decode_done)
		{
			//fprintf(stdout, "%s av_read_frame() begin.\n", getSystemTime( timeChar));
			decode_done = av_read_frame(ic, &packet);
			//fprintf(stdout, "%s av_read_frame() end. packet.size=%d stream_index=%d duration=%d\n", getSystemTime( timeChar), packet.size, packet.stream_index, packet.duration);
			//fprintf(stdout, "%s decode_done=%d\n", getSystemTime( timeChar),decode_done);
			if (!decode_done)
			{
				if (packet.stream_index != video_index
						&& packet.stream_index != audio_index)
				{
					if( ++errpktcnt >= 10)
					{
						decode_done = 1;	
					}
					fprintf(stderr, "%s packet is not video or audio, packet.stream_index=%d\n", getSystemTime( timeChar), packet.stream_index);
					av_free_packet(&packet);
					continue;
				}
				errpktcnt = 0;

				/*printf("orgin : index - %d\t pts = %s\t duration=%d\n", packet.stream_index,
				 av_ts2str(packet.pts), packet.duration);*/
				// add by houmr
				/*if (adjust_pts(&packet, video_index, audio_index) < 0)
				{
					av_free_packet(&packet);
					continue;
				}
				*/
				/////////////////////////////////////
				double timeStamp =
						(double) (packet.pts)
								* (double) (ic->streams[packet.stream_index]->time_base.num)
								/ (double) (ic->streams[packet.stream_index]->time_base.den);

				if (av_dup_packet(&packet) < 0)
				{
					fprintf(stderr, "%s Could not duplicate packet\n" ,getSystemTime( timeChar));
					av_free_packet(&packet);
					break;
				}
				
				/*
				for(int i = 0; i < streamLace->numStreams; ++i)
				{ 
						fprintf(stdout, "streamLace[%d].size=%d\t", i, streamLace->streams[i]->size);
				}
				fprintf(stdout, "\n");
				*/
				insertPacket(streamLace, &packet, timeStamp);
			}
		}

		if (countPackets(streamLace) < 50 && !decode_done)
		{
			/* allow the queue to fill up so that the packets can be sorted properly */
			continue;
		}

		if (!removePacket(streamLace, &packet))
		{
			fprintf(stdout, "%s get packet failed!!\n", getSystemTime( timeChar));
			if (decode_done)
			{
				/* the queue is empty, we are done */
				break;
			}

			assert(decode_done);
			continue;
		}

		//fprintf(stdout, "%s get 1 packet success. packet info: pts=%ld, dts=%ld\n", getSystemTime( timeChar), packet.pts, packet.dts);
		packetStartTime = (double) (packet.pts)
				* (double) (ic->streams[packet.stream_index]->time_base.num)
				/ (double) (ic->streams[packet.stream_index]->time_base.den);

		packetDuration = (double) (packet.duration)
				* (double) (ic->streams[packet.stream_index]->time_base.num)
				/ (double) (ic->streams[packet.stream_index]->time_base.den);

#if !defined(NDEBUG) && (defined(DEBUG) || defined(_DEBUG))
		if (av_log_get_level() >= AV_LOG_VERBOSE)
		fprintf(stderr,
				"%s stream %i, packet [%f, %f)\n",
				getSystemTime( timeChar),
				packet.stream_index,
				packetStartTime,
				packetStartTime + packetDuration);
#endif

		segment_duration = packetStartTime + packetDuration - prev_segment_time;

		// NOTE: segments are supposed to start on a keyframe.
		// If the keyframe interval and segment duration do not match
		// forcing the segment creation for "better seeking behavior"
		// will result in decoding artifacts after seeking or stream switching.
		if (packet.stream_index == video_index
				&& (packet.flags & AV_PKT_FLAG_KEY || strict_segment_duration))
		{
			//This is video packet and ( packet is key frame or strict time is needed )
			segment_time = packetStartTime;
		}
		else if (video_index < 0)
		{
			//This stream doesn't contain video stream
			segment_time = packetStartTime;
		}
		else
		{
			//This is a video packet or a video packet but not key frame 
			segment_time = prev_segment_time;
		}

		//fprintf(stdout, "%s extra_duration_needed=%f\n", getSystemTime( timeChar), extra_duration_needed);
		if (segment_time - prev_segment_time + segment_duration_error_tolerance
				> target_segment_duration + extra_duration_needed)
		{
			fprintf(stdout, "%s segment_time=%lf prev_segment_time=%lf  > target_segment_duration=%lf  extra_duration_needed=%lf\n", getSystemTime( timeChar), segment_time, prev_segment_time,  target_segment_duration, extra_duration_needed);
			fprintf(stdout, "%s File %s contains %d PES packet, of which %d are audio packet, %d are video packet within %d key frame.\n", getSystemTime( timeChar), output_filename, tscnt, audiopktcnt, videopktcnt, kfcnt);
			fflush(stdout);
			/*
			for(int i = 0; i < streamLace->numStreams; ++i)
			{
					fprintf(stdout, "%s streamLace[%d].size=%d\t", getSystemTime( timeChar), i, streamLace->streams[i]->size);
			}
			*/
			tscnt = audiopktcnt = videopktcnt = kfcnt = 0;
			avio_flush(oc->pb);
			avio_close(oc->pb);

			// Keep track of accumulated rounding error to account for it in later chunks.
		/*
			double segment_duration = segment_time - prev_segment_time;
			int rounded_segment_duration = (int) (segment_duration + 0.5);
			extra_duration_needed += (double) rounded_segment_duration
					- segment_duration;
		*/
			double seg_dur = segment_time - prev_segment_time;
			int rounded_segment_duration = (int) (seg_dur + 0.5);
			extra_duration_needed += (target_segment_duration - seg_dur - segment_duration_error_tolerance);
			//fprintf(stdout, "%s ________extra_duration_needed = %lf\n", getSystemTime( timeChar), extra_duration_needed);
			

			updatePlaylist(playlist, playlist_filename, output_filename,
					output_index, rounded_segment_duration);

			
			snprintf(output_filename, strlen(output_prefix) + 15, "%s-%u.ts",
					output_prefix, ++output_index);
			//add by wanggm
			//Save the all the keyframe information into json file
			if( 1 == if_save_keyframe && NULL != obj)
			{ 
				save_json_to_file(keyframeinfo_filename, obj);
				obj = info_arr_obj = NULL;

				snprintf(keyframeinfo_filename, strlen(output_prefix) + 15,
					"%s-%u.idx", output_prefix, output_index);
			}


			if (avio_open(&oc->pb, output_filename, AVIO_FLAG_WRITE) < 0)
			{
				fprintf(stderr, "%s Could not open '%s'\n", getSystemTime( timeChar), output_filename);
				break;
			}

			// close when we find the 'kill' file
			if (kill_file)
			{
				FILE* fp = fopen("kill", "rb");
				if (fp)
				{
					fprintf(stderr, "%s user abort: found kill file\n", getSystemTime( timeChar));
					fclose(fp);
					remove("kill");
					decode_done = 1;
					removeAllPackets(streamLace);
				}
			}
			prev_segment_time = segment_time;
		}

		//add by wanggm.
		++tscnt;
		if( video_index == packet.stream_index)
		{
			++videopktcnt;	
			if(1 == packet.flags)
			{ 
				++kfcnt;	
				if( 1 == if_save_keyframe)
				{
					//If it is key frame, it's information should be saved.
					//fprintf(stdout, "%s packet is keyframe, packet.pts=%ld\n", getSystemTime( timeChar), packet.pts);
					snprintf(keyframeinfo_filename, strlen(output_prefix) + 15,
					"%s-%u.idx", output_prefix, output_index);
					if (NULL == obj && NULL == info_arr_obj)
					{ 
						obj = json_object_new_object();
						info_arr_obj = create_json_header(obj);
					}
					avio_flush(oc->pb);		//flush the previous data into ts file.
					int64_t offset = avio_tell(oc->pb);	//Get the offset of this key frame in the file.
					save_keyframe_info(info_arr_obj, offset, packet.pts);
					//fprintf(stdout, "%s Keyframe.pos=%ld \tkeyframe.pts=%ld\n", getSystemTime( timeChar), offset, (long)packet.pts);
				}
			}
		}else if( audio_index == packet.stream_index)
		{
			++audiopktcnt;	
		}
		//fprintf(stdout, "%s packet is not keyframe.\n", getSystemTime( timeChar));

		ret = av_interleaved_write_frame(oc, &packet);

		if (ret < 0)
		{
			fprintf(stderr, "%s Warning: Could not write frame of stream\n", getSystemTime( timeChar));
			// add by houmr
			continue_error_cnt++;
			if (continue_error_cnt > 10)
			{
				av_free_packet(&packet);
				break;
			}
		}
		else if (ret > 0)
		{
			fprintf(stderr, "%s End of stream requested\n", getSystemTime( timeChar));
			av_free_packet(&packet);
			break;
		}
		else
		{
			// add by houmr error
			continue_error_cnt = 0;
			////////////////////////
		}
		av_free_packet(&packet);
	} while (!decode_done || countPackets(streamLace) > 0);

	av_write_trailer(oc);

	if (video_index >= 0)
	{
		avcodec_close(video_st->codec);
	}

	for (i = 0; i < oc->nb_streams; i++)
	{
		av_freep(&oc->streams[i]->codec);
		av_freep(&oc->streams[i]);
	}

	avio_close(oc->pb);
	av_free(oc);

	updatePlaylist(playlist, playlist_filename, output_filename, output_index,
			segment_duration);
	closePlaylist(playlist);
	releasePlaylist(&playlist);

	//add by wanggm
	if( 1 == if_save_keyframe && obj != NULL)
	{ 
		save_json_to_file(keyframeinfo_filename, obj);
	}

	if (pid_filename)
	{
		remove(pid_filename);
	}
	
	fflush(stdout);
	fflush(stderr);

	return 0;

	error: if (pid_filename)
	{
		remove(pid_filename);
	}

	return 1;

}

#ifdef _WIN32

//----------------------------------------------------------------
// __wgetmainargs
// 
extern void __wgetmainargs(int * argc,
		wchar_t *** argv,
		wchar_t *** env,
		int doWildCard,
		int * startInfo);

//----------------------------------------------------------------
// main
// 
int main()
{
	wchar_t ** wenpv = NULL;
	wchar_t ** wargv = NULL;
	int argc = 0;
	int startupInfo = 0;

	__wgetmainargs(&argc, &wargv, &wenpv, 1, &startupInfo);

	char ** argv = (char **)malloc(argc * sizeof(char *));
	for (int i = 0; i < argc; i++)
	{
		argv[i] = utf16_to_utf8(wargv[i]);
	}

	return main_utf8(argc, argv);
}
#else
int main(int argc, char ** argv)
{
	return main_utf8(argc, argv);
}
#endif

// vim:sw=4:tw=4:ts=4:ai:expandtab
